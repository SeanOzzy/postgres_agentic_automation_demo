"""
workload/blocker.py
====================
Blocking Query Simulator
--------------------------
This script deliberately creates a long-running transaction that holds
row-level locks on the entire ``pgbench_accounts`` table.

    SELECT * FROM pgbench_accounts FOR UPDATE;

Because the TPC-B workload ``UPDATE``s pgbench_accounts rows in every
transaction, this ``SELECT ... FOR UPDATE`` forces every concurrent TPC-B
transaction to wait for the lock — causing TPS to collapse to near-zero.

This is the antagonist in our demo scenario.  The AI agent is expected to:

1. Detect the TPS drop via the monitoring loop.
2. Investigate using the MCP tools and find this blocking PID.
3. Ask the operator to confirm a ``pg_cancel_backend`` call.
4. Cancel the query and observe TPS recover.

Usage
-----
Run this in a *separate terminal* **after** the workload is running:

    python -m workload.blocker

Press Ctrl-C to release the locks without agent intervention (useful for
resetting the demo).

Warning
-------
On a table with many rows (scale_factor ≥ 1 → 100 000 rows) this query will
scan the entire table and acquire a row-level lock on every row.  This is
intentionally expensive — that is the point of the demo.

On very large scale factors (≥ 100) the initial scan may take a long time
before locks are visible.  Scale factor 10 is recommended for demos.
"""

from __future__ import annotations

import argparse
import signal
import sys
import time
from pathlib import Path

import psycopg2
from rich.console import Console
from rich.panel import Panel

# Allow running as ``python -m workload.blocker`` from the project root
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.config import load_config

console = Console()


# def run_blocker(config_path: str | None = None) -> None:
def run_blocker(
    config_path: str | None = None,
    retry_after_cancel_seconds: int = 5,
    max_runs: int | None = None,
) -> None:
    """
    Open a transaction and hold locks on all rows of pgbench_accounts.

    The function blocks until either:
    * The PostgreSQL server cancels the query (``pg_cancel_backend``), in
      which case a ``QueryCanceled`` exception is raised and caught.
    * The user presses Ctrl-C, releasing the connection and all locks.
    * The server terminates the connection (``pg_terminate_backend``).

    Parameters
    ----------
    config_path:
        Optional path to the YAML config file.  Uses the project default if
        not specified.
    retry_after_cancel_seconds:
        Seconds to wait before retrying after a query cancellation.
    max_runs:
        Number of cancel-and-retry cycles to run.  ``None`` means run
        indefinitely until Ctrl-C or connection termination.
    """
    cfg = load_config(config_path)

    console.print(
        Panel(
            "[bold red]Blocker Script[/bold red]\n\n"
            "This will execute:\n"
            "  [bold]SELECT * FROM pgbench_accounts FOR UPDATE;[/bold]\n\n"
            "This holds row-level locks on EVERY row in pgbench_accounts,\n"
            "blocking the TPC-B workload and causing TPS to drop to ~0.\n\n"
            "[dim]Press Ctrl-C to release locks and exit.[/dim]",
            border_style="red",
        )
    )

    console.print(
        f"[dim]Connecting to {cfg.database.host}:{cfg.database.port}"
        f"/{cfg.database.dbname}...[/dim]"
    )

    try:
        conn = psycopg2.connect(**cfg.database.connection_kwargs())
    except psycopg2.OperationalError as exc:
        console.print(f"[red]Connection failed:[/red] {exc}")
        sys.exit(1)

    # autocommit=False (default) — keeps us in an explicit transaction so the
    # locks are held for as long as the connection is open.
    conn.autocommit = False

    pid_row = None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_backend_pid();")
            pid_row = cur.fetchone()
    except Exception:
        pass

    blocker_pid = pid_row[0] if pid_row else "unknown"

    console.print(
        f"[yellow]Blocker connected as PID [bold]{blocker_pid}[/bold][/yellow]\n"
        f"[dim]The agent should identify and cancel this PID.[/dim]\n"
    )

    if max_runs is None:
        console.print("[dim]Configured to run indefinitely until stopped.[/dim]\n")
    else:
        console.print(f"[dim]Configured run count: {max_runs} cycle(s).[/dim]\n")

    stop_requested = False

    def _handle_sigint(signum, frame) -> None:
        nonlocal stop_requested
        stop_requested = True
        console.print("\n[dim]Ctrl-C received — stopping blocker...[/dim]")
        try:
            # Interrupt any currently running SQL statement immediately.
            conn.cancel()
        except Exception:
            pass

    previous_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, _handle_sigint)

    runs_started = 0

    try:
        while not stop_requested and (max_runs is None or runs_started < max_runs):
            runs_started += 1
            try:
                with conn.cursor() as cur:
                    if max_runs is None:
                        console.print(f"[dim]Cycle {runs_started}: acquiring locks...[/dim]")
                    else:
                        console.print(
                            f"[dim]Cycle {runs_started}/{max_runs}: acquiring locks...[/dim]"
                        )
                    console.print("[red]Executing SELECT * FROM pgbench_accounts FOR UPDATE ...[/red]")
                    cur.execute("SELECT * FROM pgbench_accounts FOR UPDATE;")
                    row_count = cur.rowcount if cur.rowcount >= 0 else "?"
                    console.print(f"[dim]Fetched {row_count} rows (locks held).[/dim]")

                    console.print(
                        "[yellow]Locks held.[/yellow] "
                        "[dim]Waiting in server-side sleep; cancel with pg_cancel_backend(pid).[/dim]"
                    )

                    # Important: keep an active SQL statement running so pg_cancel_backend can interrupt it.
                    cur.execute("SELECT pg_sleep(86400);")

            except psycopg2.errors.QueryCanceled:
                if stop_requested:
                    break

                if max_runs is not None and runs_started >= max_runs:
                    console.print(
                        f"[green]Reached configured run count ({max_runs}). Exiting.[/green]"
                    )
                    break

                console.print(
                    "\n[green]Query was cancelled by pg_cancel_backend.[/green]\n"
                    f"[dim]Retrying SELECT ... FOR UPDATE in {retry_after_cancel_seconds}s...[/dim]"
                )
                conn.rollback()  # clear aborted transaction + release locks
                deadline = time.monotonic() + retry_after_cancel_seconds
                while time.monotonic() < deadline and not stop_requested:
                    time.sleep(0.2)
                continue

            except KeyboardInterrupt:
                console.print("\n[dim]Ctrl-C received — releasing locks.[/dim]")
                break

            except psycopg2.OperationalError as exc:
                if "terminating connection" in str(exc).lower():
                    console.print(
                        "\n[yellow]Connection terminated by server "
                        "(pg_terminate_backend was called).[/yellow]"
                    )
                else:
                    console.print(f"\n[red]Connection error:[/red] {exc}")
                break

    finally:
        signal.signal(signal.SIGINT, previous_sigint)
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass
    console.print("[dim]Connection closed. Locks released.[/dim]")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="python -m workload.blocker",
        description="Run the blocking-query simulator.",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Optional path to config.yaml",
    )
    parser.add_argument(
        "--retry-after-cancel-seconds",
        type=int,
        default=5,
        help="Seconds to wait before retrying after query cancellation",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=2,
        help="Number of blocker cycles to run (0 means unlimited)",
    )

    args = parser.parse_args()
    max_runs = None if args.runs == 0 else args.runs
    run_blocker(
        config_path=args.config,
        retry_after_cancel_seconds=args.retry_after_cancel_seconds,
        max_runs=max_runs,
    )
