#!/usr/bin/env python3
"""
main.py
========
PostgreSQL Agentic Troubleshooting Demo — CLI Entry Point
----------------------------------------------------------
This script is the single entry point for all demo commands.  It uses
``argparse`` subcommands so each phase of the demo can be started
independently in separate terminals.

Subcommands
-----------
setup
    Initialise pgbench tables in the target database.
    Run this once before the first demo.

workload
    Start the TPC-B-like pgbench workload.
    Run in **Terminal 1** and leave it running.

blocker
    Start the blocking ``SELECT ... FOR UPDATE`` query.
    Run in **Terminal 2** once the workload is producing stable TPS.

agent
    Start the AI monitoring and troubleshooting agent.
    Run in **Terminal 3**.

demo
    Run the full demo automatically in a single terminal:
    starts the workload, waits for baseline, then introduces the blocker.
    The agent is started in the foreground.

Example
-------
    # One-time setup
    python main.py setup

    # Terminal 1
    python main.py workload

    # Terminal 2 (start after workload is running)
    python main.py blocker

    # Terminal 3
    python main.py agent

Environment Variables
---------------------
ANTHROPIC_API_KEY
    Required for the ``agent`` and ``demo`` commands.
    Get yours at https://console.anthropic.com/

PG_AGENT_CONFIG
    Optional override for the config file path.
    Defaults to ``config.yaml`` in the project root.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

console = Console()

# Project root — used to find config.yaml when running as ``python main.py``
PROJECT_ROOT = Path(__file__).parent


def _load_config(args: argparse.Namespace):
    """Load config, preferring --config flag over PG_AGENT_CONFIG env var."""
    config_path = getattr(args, "config", None) or os.environ.get("PG_AGENT_CONFIG")
    from utils.config import load_config
    return load_config(config_path), Path(config_path) if config_path else PROJECT_ROOT / "config.yaml"


# ---------------------------------------------------------------------------
# setup
# ---------------------------------------------------------------------------

def cmd_setup(args: argparse.Namespace) -> None:
    """Initialise pgbench tables."""
    cfg, _ = _load_config(args)
    from workload.pgbench_runner import initialize_pgbench
    initialize_pgbench(cfg)


# ---------------------------------------------------------------------------
# workload
# ---------------------------------------------------------------------------

def cmd_workload(args: argparse.Namespace) -> None:
    """Start the TPC-B pgbench workload."""
    cfg, _ = _load_config(args)
    from workload.pgbench_runner import run_tpcb_workload
    run_tpcb_workload(cfg)


# ---------------------------------------------------------------------------
# blocker
# ---------------------------------------------------------------------------

def cmd_blocker(args: argparse.Namespace) -> None:
    """Start the blocking SELECT ... FOR UPDATE transaction."""
    _, config_path = _load_config(args)
    from workload.blocker import run_blocker
    max_runs = None if args.runs == 0 else args.runs
    run_blocker(
        config_path=str(config_path),
        retry_after_cancel_seconds=args.retry_after_cancel_seconds,
        max_runs=max_runs,
    )


# ---------------------------------------------------------------------------
# agent
# ---------------------------------------------------------------------------

def cmd_agent(args: argparse.Namespace) -> None:
    """Start the AI monitoring and troubleshooting agent."""
    _require_api_key()
    cfg, config_path = _load_config(args)
    from agent.troubleshoot_agent import TroubleshootAgent
    agent = TroubleshootAgent(cfg, config_path)
    try:
        asyncio.run(agent.run())
    except KeyboardInterrupt:
        console.print("\n[dim]Agent stopped.[/dim]")


# ---------------------------------------------------------------------------
# demo (orchestrated single-terminal demo)
# ---------------------------------------------------------------------------

def cmd_demo(args: argparse.Namespace) -> None:
    """
    Run the full demonstration in a single terminal.

    Correct phase ordering — critical for baseline accuracy
    --------------------------------------------------------
    The agent must collect its TPS baseline **before** the blocking query is
    introduced.  The ordering is therefore:

    1. [optional] pgbench -i  (--setup flag)
    2. Start pgbench TPC-B workload in a background subprocess.
    3. Start the AI agent in a **background thread** so it begins sampling
       TPS immediately against the healthy workload.
    4. Sleep for the baseline warm-up window (baseline_samples × interval + buffer).
       During this sleep the agent is quietly building its normal-TPS baseline.
    5. Introduce the blocker subprocess.  The agent now sees TPS collapse
       relative to the baseline it already established, triggers an
       investigation, and asks for operator confirmation before cancelling.

    The agent runs in a daemon thread so Ctrl-C in the main thread cleanly
    terminates all child processes and the event loop.
    """
    _require_api_key()
    cfg, config_path = _load_config(args)

    # Baseline window: enough time for the agent to collect its samples plus a
    # small extra buffer for pgbench to ramp up from a cold start.
    baseline_wait = (
        cfg.monitoring.baseline_samples * cfg.monitoring.check_interval_seconds + 15
    )

    console.print(
        Panel(
            "[bold green]PostgreSQL Agentic Troubleshooting Demo[/bold green]\n\n"
            "Phase order:\n"
            "  1. TPC-B workload starts\n"
            "  2. Agent starts immediately — collects TPS baseline "
            f"({baseline_wait}s warm-up)\n"
            "  3. Blocking query introduced — agent detects the TPS drop\n"
            "  4. Agent investigates and asks for confirmation to cancel\n\n"
            "[dim]Press Ctrl-C at any time to stop everything.[/dim]",
            border_style="green",
            title="🐘  Demo",
        )
    )

    # Optional setup phase
    if getattr(args, "setup", False):
        console.print("\n[cyan]Phase 0: Setting up pgbench tables...[/cyan]")
        from workload.pgbench_runner import initialize_pgbench
        initialize_pgbench(cfg)

    env = {**os.environ, "PG_AGENT_CONFIG": str(config_path)}

    # ---- Phase 1: Start workload ----
    console.print("\n[cyan]Phase 1: Starting TPC-B workload...[/cyan]")
    workload_proc = subprocess.Popen(
        [sys.executable, "-m", "workload.pgbench_runner"],
        env=env,
        cwd=str(PROJECT_ROOT),
    )
    # Brief pause so pgbench connections are established before the agent
    # starts sampling pg_stat_database.
    time.sleep(3)

    # ---- Phase 2: Start the agent in a background daemon thread ----
    # The agent runs its own asyncio event loop inside the thread.
    # daemon=True means the thread is automatically killed when the main
    # thread exits (e.g. on Ctrl-C), so no explicit join is needed for cleanup.
    console.print(
        f"\n[cyan]Phase 2: Starting AI agent (background thread)...[/cyan]\n"
        f"[dim]The agent will collect a TPS baseline over the next "
        f"{baseline_wait}s before the blocker is introduced.[/dim]\n"
    )
    from agent.troubleshoot_agent import TroubleshootAgent
    agent = TroubleshootAgent(cfg, config_path)
    agent_thread = threading.Thread(
        target=lambda: asyncio.run(agent.run()),
        daemon=True,
        name="agent",
    )
    agent_thread.start()

    # ---- Wait for baseline to be established ----
    # The agent is now running and sampling TPS.  We wait the full baseline
    # window so that by the time the blocker appears, the agent has a clean
    # picture of normal throughput.
    console.print(
        f"[dim]Main thread sleeping {baseline_wait}s while agent establishes "
        f"baseline...[/dim]"
    )
    try:
        time.sleep(baseline_wait)
    except KeyboardInterrupt:
        console.print("\n[dim]Demo stopped during baseline collection.[/dim]")
        _stop_procs([(workload_proc, "workload")])
        return

    # ---- Phase 3: Introduce the blocker ----
    console.print("\n[cyan]Phase 3: Introducing the blocking query...[/cyan]")
    blocker_proc = subprocess.Popen(
        [sys.executable, "-m", "workload.blocker"],
        env=env,
        cwd=str(PROJECT_ROOT),
    )

    # ---- Keep running until the agent finishes or Ctrl-C ----
    try:
        agent_thread.join()
    except KeyboardInterrupt:
        console.print("\n[dim]Demo stopped by user.[/dim]")
    finally:
        _stop_procs([(workload_proc, "workload"), (blocker_proc, "blocker")])

    console.print("[green]Demo finished.[/green]")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stop_procs(procs: list[tuple]) -> None:
    """Terminate a list of (subprocess.Popen, name) pairs gracefully."""
    for proc, name in procs:
        if proc is not None and proc.poll() is None:
            console.print(f"[dim]Stopping {name} process (PID {proc.pid})...[/dim]")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


def _require_api_key() -> None:
    """Exit with a helpful message if ANTHROPIC_API_KEY is not set."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        console.print(
            Panel(
                "[red]ANTHROPIC_API_KEY is not set.[/red]\n\n"
                "Export your API key before running the agent:\n\n"
                "  [bold]export ANTHROPIC_API_KEY=sk-ant-...[/bold]\n\n"
                "Get your key at https://console.anthropic.com/",
                border_style="red",
                title="Missing API Key",
            )
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# CLI definition
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description=(
            "PostgreSQL Agentic Troubleshooting Demo\n\n"
            "Use the subcommands below to run each phase of the demo.\n"
            "See README.md for the full walkthrough."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Global option: config file path
    parser.add_argument(
        "--config",
        metavar="PATH",
        default=None,
        help="Path to config.yaml (default: ./config.yaml or $PG_AGENT_CONFIG)",
    )

    subparsers = parser.add_subparsers(dest="command", title="commands")
    subparsers.required = True

    # setup
    p_setup = subparsers.add_parser("setup", help="Initialise pgbench tables")
    p_setup.set_defaults(func=cmd_setup)

    # workload
    p_workload = subparsers.add_parser(
        "workload", help="Run the TPC-B pgbench workload (Terminal 1)"
    )
    p_workload.set_defaults(func=cmd_workload)

    # blocker
    p_blocker = subparsers.add_parser(
        "blocker",
        help="Start the blocking SELECT ... FOR UPDATE (Terminal 2)",
    )
    p_blocker.add_argument(
        "--runs",
        type=int,
        default=0,
        help="Number of blocker cycles to run (0 means unlimited)",
    )
    p_blocker.add_argument(
        "--retry-after-cancel-seconds",
        type=int,
        default=5,
        help="Seconds to wait before retrying after cancellation",
    )
    p_blocker.set_defaults(func=cmd_blocker)

    # agent
    p_agent = subparsers.add_parser(
        "agent", help="Start the AI monitoring agent (Terminal 3)"
    )
    p_agent.set_defaults(func=cmd_agent)

    # demo
    p_demo = subparsers.add_parser(
        "demo",
        help="Run the full demo automatically (single terminal)",
    )
    p_demo.add_argument(
        "--setup",
        action="store_true",
        help="Run pgbench -i before starting the demo",
    )
    p_demo.set_defaults(func=cmd_demo)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
