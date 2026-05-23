"""
workload/pgbench_runner.py
===========================
pgbench Workload Manager
--------------------------
Wraps the ``pgbench`` command-line tool to provide:

* ``initialize_pgbench`` — creates and populates the standard TPC-B tables
  (``pgbench_accounts``, ``pgbench_branches``, ``pgbench_tellers``,
  ``pgbench_history``) using ``pgbench -i``.

* ``run_tpcb_workload`` — starts the TPC-B-like read/write workload as a
  subprocess.  Progress lines (``-P``) are printed to stdout so the operator
  can see live TPS.

The TPC-B workload issues, per transaction:

    BEGIN;
      UPDATE pgbench_accounts SET abalance = abalance + :delta
        WHERE aid = :aid;
      UPDATE pgbench_tellers  SET tbalance = tbalance + :delta
        WHERE tid = :tid;
      UPDATE pgbench_branches SET bbalance = bbalance + :delta
        WHERE bid = :bid;
      INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
        VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
    END;

This workload acquires row-level locks on pgbench_accounts.  A separate
connection running ``SELECT * FROM pgbench_accounts FOR UPDATE`` will block
every TPC-B transaction, causing TPS to collapse — exactly the scenario the
agent is designed to detect and resolve.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

from rich.console import Console

from utils.config import AppConfig

console = Console()


def _build_pgbench_env(config: AppConfig) -> dict:
    """
    Build environment variables for pgbench from the database config.

    pgbench honours the standard libpq environment variables (PGHOST, PGPORT,
    etc.), so we use those rather than passing flags.  This also avoids
    passwords appearing in ``ps aux`` output.
    """
    import os
    env = os.environ.copy()
    env["PGHOST"] = config.database.host
    env["PGPORT"] = str(config.database.port)
    env["PGUSER"] = config.database.user
    env["PGDATABASE"] = config.database.dbname
    if config.database.password:
        env["PGPASSWORD"] = config.database.password
    return env


def _check_pgbench_installed() -> None:
    """Raise ``RuntimeError`` with a helpful message if pgbench is not on PATH."""
    if shutil.which("pgbench") is None:
        raise RuntimeError(
            "pgbench not found on PATH.\n"
            "  On Debian/Ubuntu : sudo apt install postgresql-client\n"
            "  On RHEL/Fedora   : sudo dnf install postgresql\n"
            "  On macOS (Homebrew): brew install postgresql\n"
            "  On macOS (Postgres.app): add /Applications/Postgres.app/Contents/Versions/latest/bin to PATH"
        )


def initialize_pgbench(config: AppConfig) -> None:
    """
    Initialise the pgbench schema and load data.

    Equivalent to::

        pgbench -i -s <scale_factor> <dbname>

    This creates the four standard TPC-B tables and populates them with
    ``scale_factor × 100 000`` rows in ``pgbench_accounts``.  Scale factor 10
    produces ~100 000 account rows and roughly 1 GB of data — a good size for
    a laptop demo.

    Parameters
    ----------
    config:
        Application config (uses ``config.database`` and ``config.workload``).

    Raises
    ------
    RuntimeError
        If pgbench is not installed or the initialisation command fails.
    """
    _check_pgbench_installed()

    scale = config.workload.scale_factor
    console.print(
        f"[cyan]Initialising pgbench with scale factor {scale}...[/cyan]\n"
        f"[dim]This may take a minute for larger scale factors.[/dim]"
    )

    cmd = [
        "pgbench",
        "--initialize",
        f"--scale={scale}",
        config.database.dbname,
    ]

    result = subprocess.run(
        cmd,
        env=_build_pgbench_env(config),
        capture_output=False,  # Let output go to the terminal
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"pgbench -i exited with code {result.returncode}.  "
            "Check that the database exists and the user has CREATE TABLE permission."
        )

    console.print("[green]pgbench tables initialised successfully.[/green]")


def run_tpcb_workload(config: AppConfig) -> None:
    """
    Run the TPC-B-like workload until the configured duration expires or
    the user presses Ctrl-C.

    The workload runs pgbench in a subprocess with:
    * ``-c`` clients (concurrent connections)
    * ``-j`` threads (worker threads inside pgbench)
    * ``-T`` duration in seconds (0 = unlimited, loop until Ctrl-C)
    * ``-P`` progress output every N seconds (so the operator sees live TPS)

    Equivalent to::

        pgbench -c 5 -j 2 -T 300 -P 5 postgres

    Parameters
    ----------
    config:
        Application config.

    Raises
    ------
    RuntimeError
        If pgbench is not installed.
    KeyboardInterrupt
        Propagated from the subprocess when the user presses Ctrl-C.
    """
    _check_pgbench_installed()

    wl = config.workload
    duration_flag = f"--time={wl.duration_seconds}" if wl.duration_seconds > 0 else "--time=86400"

    cmd = [
        "pgbench",
        f"--client={wl.num_clients}",
        f"--jobs={wl.num_threads}",
        duration_flag,
        f"--progress={wl.progress_interval_seconds}",
        config.database.dbname,
    ]

    console.print(
        f"[cyan]Starting TPC-B workload:[/cyan] "
        f"{wl.num_clients} clients, {wl.num_threads} threads, "
        f"{'unlimited' if wl.duration_seconds == 0 else str(wl.duration_seconds) + 's'} duration\n"
        f"[dim]Press Ctrl-C to stop.[/dim]"
    )
    console.print(f"[dim]Command: {' '.join(cmd)}[/dim]\n")

    try:
        subprocess.run(cmd, env=_build_pgbench_env(config))
    except KeyboardInterrupt:
        console.print("\n[dim]pgbench workload stopped.[/dim]")


# ---------------------------------------------------------------------------
# Module entry point (used by demo subcommand: python -m workload.pgbench_runner)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from utils.config import load_config
    cfg = load_config()
    run_tpcb_workload(cfg)
