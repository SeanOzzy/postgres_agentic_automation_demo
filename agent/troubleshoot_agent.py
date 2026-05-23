"""
agent/troubleshoot_agent.py
============================
PostgreSQL AI Troubleshooting Agent
-------------------------------------
This module implements the core agentic loop.  It has two distinct layers:

**Monitoring layer (pure Python)**
    Samples ``pg_stat_database`` every ``check_interval_seconds`` seconds,
    computes TPS from the delta of cumulative transaction counters, and
    maintains a rolling baseline.  When TPS drops below
    ``baseline_tps × tps_drop_threshold`` an investigation is triggered.

**Investigation layer (Claude Haiku + MCP tools)**
    Invokes the Anthropic API with Claude Haiku.  The model receives the
    anomaly context and reasons through it by calling MCP tools
    (``get_blocking_locks``, ``get_active_queries``, etc.).  When the model
    decides to cancel a backend it calls ``cancel_backend(pid)``; the agent
    framework intercepts this, asks the human operator for confirmation, and
    only then forwards the call to the MCP server.

High-level architecture::

    ┌─────────────────────────────────┐
    │  TroubleshootAgent              │
    │                                 │
    │  ┌─────────────┐                │
    │  │ Monitor loop│ samples TPS    │
    │  │ (asyncio)   │ builds baseline│
    │  └──────┬──────┘                │
    │         │ anomaly detected      │
    │  ┌──────▼──────────────────┐    │
    │  │ Investigation session   │    │
    │  │                         │    │
    │  │  ┌──────────────────┐   │    │
    │  │  │ Claude Haiku      │   │    │
    │  │  │ (Anthropic SDK)   │   │    │
    │  │  └────────┬─────────┘   │    │
    │  │           │ tool_use     │    │
    │  │  ┌────────▼─────────┐   │    │
    │  │  │ MCP tool dispatch │   │    │
    │  │  │  ├─ read tools ──┼───┼────┼──► MCP server subprocess
    │  │  │  └─ cancel_back  │   │    │
    │  │  │     ─► confirm? ─┼───┼────┼──► Human (stdin prompt)
    │  │  └──────────────────┘   │    │
    │  └─────────────────────────┘    │
    └─────────────────────────────────┘

Usage
-----
    import asyncio
    from agent.troubleshoot_agent import TroubleshootAgent
    from utils.config import load_config

    async def main():
        agent = TroubleshootAgent(load_config())
        await agent.run()

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncGenerator, Deque, Dict, List, Optional, Tuple

import anthropic
import psycopg2
import psycopg2.extras
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm
from rich.table import Table
from rich import box

from utils.config import AppConfig, load_config

# ---------------------------------------------------------------------------
# Rich console (shared across the module)
# ---------------------------------------------------------------------------
console = Console()


# ---------------------------------------------------------------------------
# System prompt — tells Claude Haiku its role and reasoning strategy
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are an expert PostgreSQL performance troubleshooting agent.

Your job is to diagnose why the database TPS (transactions per second) has
dropped significantly, and then take corrective action using the tools
available to you.

## Investigation strategy

Follow these steps methodically:

1. **Assess the situation** — Briefly acknowledge the TPS drop data you have
   been given.
2. **Check for lock conflicts** — Call `get_blocking_locks` first.  Lock
   contention is the most common cause of sudden TPS drops.
3. **Inspect active queries** — Call `get_active_queries` to see all non-idle
   backends, their states, and wait events.
4. **Review table-level locks** — Call `get_table_lock_stats` if you need
   more detail about which tables are hot-spots.
5. **Form a hypothesis** — Explain what is happening in plain language,
   quoting relevant query text and PID numbers from the tool output.
6. **Act** — Call `cancel_backend(pid)` for the blocking PID.

## CRITICAL — how `cancel_backend` works in this system

**DO NOT ask "shall I proceed?", "do you want me to cancel?", or any
variation of requesting permission in your text response.**

The moment you call the `cancel_backend` tool the framework AUTOMATICALLY
pauses execution and asks the human operator for confirmation before the
cancel signal is ever sent to PostgreSQL.  You do not need to ask — the
gate is built into the tool itself.

Your only job is to identify the right PID and call the tool.
The human operator decides whether to approve or deny.
If you write a permission question in text instead of calling the tool,
the framework has no way to intercept it and the operator is never prompted.

## Output style

* Use clear headings and bullet points.
* Quote relevant query snippets and PID numbers from tool output.
* State your reasoning before calling `cancel_backend` so the operator can
  read it in the confirmation panel and make an informed decision.
* After `cancel_backend` returns, summarise the outcome and advise the
  operator to watch TPS recover.

## Constraints

* Only call `cancel_backend` when you are confident the blocking query is
  not legitimate business activity.  A `SELECT ... FOR UPDATE` with no
  `WHERE` clause on a large table that has been running for many seconds is
  a strong signal.
* Never guess PIDs — always derive them from tool output.
* If the tools show no lock conflicts, say so clearly and suggest the
  operator re-run the agent if symptoms persist.
"""


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TpsSample:
    """A single TPS measurement derived from two pg_stat_database snapshots."""
    timestamp: float          # Unix epoch of the measurement
    tps: float                # Transactions per second
    total_xacts: int          # Cumulative total at the time of measurement
    num_backends: int         # Active backend connections


@dataclass
class InvestigationContext:
    """Data passed to Claude Haiku when an anomaly is detected."""
    current_tps: float
    baseline_tps: float
    drop_pct: float           # How much TPS has fallen (0–100 %)
    recent_samples: List[TpsSample]


# ---------------------------------------------------------------------------
# MCP ↔ Anthropic format conversion helpers
# ---------------------------------------------------------------------------

def _mcp_tool_to_anthropic(mcp_tool) -> Dict[str, Any]:
    """
    Convert an ``mcp.types.Tool`` object to the dict shape expected by the
    Anthropic messages API ``tools`` parameter.
    """
    return {
        "name": mcp_tool.name,
        "description": mcp_tool.description or "",
        "input_schema": (
            mcp_tool.inputSchema
            if isinstance(mcp_tool.inputSchema, dict)
            else {"type": "object", "properties": {}}
        ),
    }


def _extract_text_from_mcp_result(result) -> str:
    """
    Pull the text content from an MCP ``CallToolResult``.

    MCP tool results carry a list of content blocks; we join all TextContent
    blocks into a single string for the Anthropic tool_result message.
    """
    parts = []
    for block in result.content:
        if hasattr(block, "text"):
            parts.append(block.text)
        else:
            parts.append(str(block))
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Main agent class
# ---------------------------------------------------------------------------

class TroubleshootAgent:
    """
    Asynchronous PostgreSQL troubleshooting agent.

    Parameters
    ----------
    config:
        Loaded ``AppConfig`` from ``utils.config.load_config()``.
    config_path:
        Path to the YAML config file — passed to the MCP server subprocess so
        it can find the database connection details.
    """

    def __init__(
        self,
        config: AppConfig,
        config_path: Optional[Path] = None,
    ) -> None:
        self.config = config
        self.config_path = config_path or (
            Path(__file__).parent.parent / "config.yaml"
        )

        # TPS history — a rolling window of recent samples
        self._tps_history: Deque[TpsSample] = deque(maxlen=120)

        # Baseline — set once after the warm-up period
        self._baseline_tps: Optional[float] = None

        # Prevent back-to-back investigations for the same symptom
        self._last_investigation_ts: float = 0.0

        # Anthropic client (reads ANTHROPIC_API_KEY from the environment)
        self._anthropic = anthropic.Anthropic()

        # Set by _mcp_context() once the server subprocess is running
        self._mcp_session: Optional[ClientSession] = None
        self._mcp_tools: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # MCP server lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def _mcp_context(self) -> AsyncGenerator[None, None]:
        """
        Async context manager that starts the MCP server subprocess and
        initialises the MCP client session.

        The server process inherits the current environment with
        ``PG_AGENT_CONFIG`` overridden so it finds the right config file.
        """
        server_script = Path(__file__).parent.parent / "mcp_server" / "pg_diagnostic_server.py"

        server_params = StdioServerParameters(
            command=sys.executable,  # same Python interpreter
            args=[str(server_script)],
            env={**os.environ, "PG_AGENT_CONFIG": str(self.config_path)},
        )

        console.print("[dim]Starting MCP diagnostic server...[/dim]")
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                # Fetch available tools and convert to Anthropic format
                tools_response = await session.list_tools()
                self._mcp_tools = [
                    _mcp_tool_to_anthropic(t) for t in tools_response.tools
                ]
                self._mcp_session = session

                console.print(
                    f"[green]MCP server ready[/green] — "
                    f"{len(self._mcp_tools)} tools: "
                    + ", ".join(f"[bold]{t['name']}[/bold]" for t in self._mcp_tools)
                )
                yield

    # ------------------------------------------------------------------
    # TPS sampling from pg_stat_database
    # ------------------------------------------------------------------

    def _sample_pg_stat_database(self) -> Tuple[int, int]:
        """
        Query ``pg_stat_database`` once and return
        ``(total_xacts, num_backends)``.

        Uses a short-lived psycopg2 connection — no pool needed for a 5-second
        polling interval.
        """
        sql = """
            SELECT
                xact_commit + xact_rollback AS total_xacts,
                numbackends
            FROM pg_stat_database
            WHERE datname = current_database();
        """
        conn = psycopg2.connect(**self.config.database.connection_kwargs())
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                return (int(row[0]), int(row[1])) if row else (0, 0)
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Monitoring loop
    # ------------------------------------------------------------------

    async def _monitoring_loop(self) -> None:
        """
        Background task: sample TPS every ``check_interval_seconds`` and
        trigger investigations when anomalies are detected.

        TPS is derived from the *delta* of ``xact_commit + xact_rollback``
        across two consecutive samples divided by the elapsed wall-clock time.
        This is accurate even under high-frequency DDL or background vacuums
        because it captures all transaction activity for the target database.
        """
        cfg = self.config.monitoring
        prev_xacts: Optional[int] = None
        prev_time: Optional[float] = None

        console.print(
            Panel(
                f"[bold cyan]Monitoring started[/bold cyan]\n"
                f"  Interval : {cfg.check_interval_seconds}s\n"
                f"  Baseline : {cfg.baseline_samples} samples "
                f"({cfg.baseline_samples * cfg.check_interval_seconds}s warm-up)\n"
                f"  Alert if : TPS < baseline × {cfg.tps_drop_threshold}",
                title="TPS Monitor",
                border_style="cyan",
            )
        )

        sample_count = 0

        while True:
            await asyncio.sleep(cfg.check_interval_seconds)

            try:
                total_xacts, num_backends = self._sample_pg_stat_database()
            except Exception as exc:
                console.print(f"[red]DB sample error:[/red] {exc}")
                continue

            now = time.monotonic()

            if prev_xacts is not None and prev_time is not None:
                elapsed = now - prev_time
                delta_xacts = total_xacts - prev_xacts
                tps = delta_xacts / elapsed if elapsed > 0 else 0.0

                sample = TpsSample(
                    timestamp=time.time(),
                    tps=round(tps, 2),
                    total_xacts=total_xacts,
                    num_backends=num_backends,
                )
                self._tps_history.append(sample)
                sample_count += 1

                # ---- Determine baseline ----
                if self._baseline_tps is None:
                    if sample_count >= cfg.baseline_samples:
                        recent = list(self._tps_history)[-cfg.baseline_samples:]
                        self._baseline_tps = sum(s.tps for s in recent) / len(recent)
                        console.print(
                            f"[green]Baseline established:[/green] "
                            f"{self._baseline_tps:.1f} TPS "
                            f"(avg of last {cfg.baseline_samples} samples)"
                        )
                    else:
                        console.print(
                            f"[dim]Baseline sample {sample_count}/{cfg.baseline_samples}:"
                            f" {tps:.1f} TPS[/dim]"
                        )
                else:
                    # ---- Live TPS display ----
                    status_color = "green"
                    threshold_tps = self._baseline_tps * cfg.tps_drop_threshold
                    if tps < threshold_tps:
                        status_color = "red"
                    elif tps < self._baseline_tps * 0.8:
                        status_color = "yellow"

                    console.print(
                        f"[{status_color}]TPS: {tps:6.1f}[/{status_color}]"
                        f"  (baseline: {self._baseline_tps:.1f},"
                        f"  backends: {num_backends})"
                    )

                    # ---- Anomaly detection ----
                    if tps < threshold_tps:
                        cooldown_remaining = (
                            self._last_investigation_ts
                            + cfg.investigation_cooldown_seconds
                            - time.time()
                        )
                        if cooldown_remaining > 0:
                            console.print(
                                f"[yellow]TPS anomaly detected but in cooldown "
                                f"({cooldown_remaining:.0f}s remaining)[/yellow]"
                            )
                        else:
                            drop_pct = (
                                (self._baseline_tps - tps) / self._baseline_tps * 100
                            )
                            console.print(
                                Panel(
                                    f"[bold red]TPS ANOMALY DETECTED[/bold red]\n"
                                    f"  Current TPS : {tps:.1f}\n"
                                    f"  Baseline    : {self._baseline_tps:.1f}\n"
                                    f"  Drop        : {drop_pct:.1f}%",
                                    title="⚠  Alert",
                                    border_style="red",
                                )
                            )
                            ctx = InvestigationContext(
                                current_tps=tps,
                                baseline_tps=self._baseline_tps,
                                drop_pct=drop_pct,
                                recent_samples=list(self._tps_history)[-10:],
                            )
                            # Run investigation in the same event loop without
                            # blocking the monitoring tick.
                            asyncio.create_task(self._run_investigation(ctx))

            prev_xacts = total_xacts
            prev_time = now

    # ------------------------------------------------------------------
    # Investigation loop (Claude Haiku + MCP tools)
    # ------------------------------------------------------------------

    async def _run_investigation(self, ctx: InvestigationContext) -> None:
        """
        Launch a Claude Haiku investigation session for the given anomaly.

        The session continues until Claude either:
        * Produces a final text response (``stop_reason == "end_turn"``), or
        * Reaches the ``max_investigation_turns`` limit.

        Tool calls are routed to the MCP server.  ``cancel_backend`` is gated
        behind a human confirmation prompt before it is forwarded.
        """
        self._last_investigation_ts = time.time()

        console.print(
            Panel(
                "[bold yellow]Starting AI investigation...[/bold yellow]",
                border_style="yellow",
            )
        )

        # Build the initial user message
        sample_lines = "\n".join(
            f"  {i+1}. TPS={s.tps:.1f}  backends={s.num_backends}  "
            f"ts={time.strftime('%H:%M:%S', time.localtime(s.timestamp))}"
            for i, s in enumerate(ctx.recent_samples)
        )
        user_message = (
            f"## TPS Anomaly Report\n\n"
            f"The database TPS has dropped significantly:\n\n"
            f"- **Current TPS**: {ctx.current_tps:.1f}\n"
            f"- **Baseline TPS**: {ctx.baseline_tps:.1f}\n"
            f"- **Drop**: {ctx.drop_pct:.1f}%\n\n"
            f"### Recent TPS samples (most recent last)\n\n"
            f"{sample_lines}\n\n"
            f"Please investigate the root cause and fix it if appropriate."
        )

        messages: List[Dict[str, Any]] = [
            {"role": "user", "content": user_message}
        ]

        turns = 0

        while turns < self.config.agent.max_investigation_turns:
            turns += 1

            # ----- Call Claude Haiku -----
            console.print(f"[dim]Investigation turn {turns}...[/dim]")
            try:
                # Capture messages snapshot for the lambda to avoid
                # closure-over-mutable-variable issues across turns.
                _msgs = list(messages)
                response = await asyncio.get_running_loop().run_in_executor(
                    None,
                    lambda: self._anthropic.messages.create(
                        model=self.config.agent.model,
                        max_tokens=self.config.agent.max_tokens,
                        system=SYSTEM_PROMPT,
                        tools=self._mcp_tools,
                        messages=_msgs,
                    ),
                )
            except anthropic.APIError as exc:
                console.print(f"[red]Anthropic API error:[/red] {exc}")
                break

            # ----- Display text blocks -----
            for block in response.content:
                if block.type == "text" and block.text.strip():
                    console.print(
                        Panel(
                            block.text,
                            title="[bold blue]Agent Analysis[/bold blue]",
                            border_style="blue",
                            expand=False,
                        )
                    )

            # ----- Terminal condition -----
            if response.stop_reason != "tool_use":
                console.print(
                    "[green]Investigation complete.[/green]"
                )
                break

            # ----- Handle tool calls -----
            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue

                tool_name = block.name
                tool_input = block.input or {}
                tool_use_id = block.id

                console.print(
                    f"[cyan]Tool call:[/cyan] [bold]{tool_name}[/bold]"
                    + (f"({tool_input})" if tool_input else "()")
                )

                # Gate destructive tools behind human confirmation
                if tool_name in self.config.agent.requires_confirmation:
                    result_text = await self._confirm_and_execute_tool(
                        tool_name, tool_input
                    )
                else:
                    result_text = await self._execute_mcp_tool(tool_name, tool_input)

                console.print(f"[dim]Tool result (truncated): {result_text[:300]}[/dim]")

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": result_text,
                })

            # Append assistant turn + tool results to the conversation
            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})

        else:
            console.print(
                f"[yellow]Investigation reached the "
                f"{self.config.agent.max_investigation_turns}-turn limit.[/yellow]"
            )

    # ------------------------------------------------------------------
    # Tool execution helpers
    # ------------------------------------------------------------------

    async def _execute_mcp_tool(
        self, tool_name: str, tool_input: Dict[str, Any]
    ) -> str:
        """Forward a tool call to the MCP server and return the result as text."""
        if self._mcp_session is None:
            return json.dumps({"error": "MCP session not initialised"})
        try:
            result = await self._mcp_session.call_tool(tool_name, tool_input)
            return _extract_text_from_mcp_result(result)
        except Exception as exc:
            return json.dumps({"error": str(exc)})

    async def _confirm_and_execute_tool(
        self, tool_name: str, tool_input: Dict[str, Any]
    ) -> str:
        """
        Ask the human operator for confirmation before executing a
        potentially-destructive tool.

        Runs ``rich.prompt.Confirm`` in a thread-pool executor so the asyncio
        event loop (and the monitoring loop) keep ticking while we wait for
        the operator to respond.
        """
        pid = tool_input.get("pid", "?")
        console.print(
            f"\n[bold red]⚠  Action required[/bold red]\n"
            f"The agent wants to call [bold]{tool_name}(pid={pid})[/bold].\n"
            f"This will cancel the current query of PID {pid}.\n"
        )

        # Run the blocking prompt in a thread so the event loop keeps ticking.
        confirmed: bool = await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: Confirm.ask(
                f"Proceed with pg_cancel_backend({pid})?",
                default=False,
            ),
        )

        if confirmed:
            console.print(f"[green]Confirmed. Executing {tool_name}({tool_input})...[/green]")
            return await self._execute_mcp_tool(tool_name, tool_input)
        else:
            console.print(f"[yellow]Cancelled by operator.[/yellow]")
            return json.dumps({
                "status": "cancelled",
                "message": f"Operator declined to execute {tool_name}({tool_input}).",
            })

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Start the agent.

        Brings up the MCP server subprocess, then runs the monitoring loop
        indefinitely (until Ctrl-C).
        """
        console.print(
            Panel(
                "[bold green]PostgreSQL Agentic Troubleshooter[/bold green]\n\n"
                f"  Model   : {self.config.agent.model}\n"
                f"  Target  : {self.config.database.host}:{self.config.database.port}"
                f"/{self.config.database.dbname}\n"
                f"  Config  : {self.config_path}",
                title="🤖  Agent Starting",
                border_style="green",
            )
        )

        async with self._mcp_context():
            try:
                await self._monitoring_loop()
            except asyncio.CancelledError:
                console.print("[dim]Agent monitoring loop cancelled.[/dim]")
            except KeyboardInterrupt:
                console.print("\n[dim]Shutting down agent.[/dim]")
