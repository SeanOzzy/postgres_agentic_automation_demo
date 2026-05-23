"""
mcp_server/pg_diagnostic_server.py
====================================
PostgreSQL Diagnostic MCP Server
---------------------------------
This module implements a `Model Context Protocol (MCP)
<https://modelcontextprotocol.io>`_ server that exposes PostgreSQL diagnostic
capabilities as callable *tools*.  The AI agent uses these tools to:

* Observe live database performance metrics (TPS, connections, cache hit rate)
* Identify queries that are blocking other queries (lock trees)
* Inspect active sessions and their wait events
* Safely cancel a blocking backend — after human confirmation in the agent

Architecture
~~~~~~~~~~~~
The server is started as a **subprocess** by the agent (stdio transport).
Communication happens over stdin/stdout using the MCP wire protocol.
The server is stateless: every tool call opens a fresh psycopg2 connection,
executes, and closes it.  This keeps the server simple and avoids pooling
complexity for a demo.

Running standalone (for development / testing)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PG_AGENT_CONFIG=config.yaml python -m mcp_server.pg_diagnostic_server

Environment Variables
~~~~~~~~~~~~~~~~~~~~~
PG_AGENT_CONFIG
    Path to the project ``config.yaml``.  Defaults to ``config.yaml``
    in the project root (two levels above this file).
"""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from typing import Any, Generator

import psycopg2
import psycopg2.extras
from mcp.server.fastmcp import FastMCP

# Project-level config loader (works whether run as a module or subprocess)
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
from utils.config import load_config

# ---------------------------------------------------------------------------
# Server initialisation
# ---------------------------------------------------------------------------

mcp = FastMCP(
    name="PostgreSQL Diagnostics",
    instructions=(
        "Tools for diagnosing PostgreSQL performance issues: blocking locks, "
        "active queries, database metrics, and backend cancellation."
    ),
)


# ---------------------------------------------------------------------------
# Database connection helper
# ---------------------------------------------------------------------------

@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Context manager that yields a psycopg2 connection using the project config.

    The connection is always closed on exit, even if an exception is raised.
    Uses ``RealDictCursor`` so rows are returned as plain ``dict`` objects.
    """
    cfg = load_config()
    conn = psycopg2.connect(**cfg.database.connection_kwargs())
    conn.set_session(readonly=False, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


def _rows_to_json(rows: list[dict]) -> str:
    """Serialise a list of row dicts to a JSON string, handling non-serialisable types."""
    def _default(obj: Any) -> Any:
        # psycopg2 returns datetimes, Decimals, etc.
        import datetime, decimal
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.timedelta)):
            return str(obj)
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return repr(obj)

    return json.dumps(rows, default=_default, indent=2)


# ---------------------------------------------------------------------------
# Tool: get_database_metrics
# ---------------------------------------------------------------------------

@mcp.tool()
def get_database_metrics() -> str:
    """
    Return current performance metrics for the monitored PostgreSQL database.

    Metrics include:
    - ``xact_commit`` / ``xact_rollback``: cumulative transaction counters
      (take two samples separated by time to derive TPS)
    - ``numbackends``: active client connections
    - ``blks_hit`` / ``blks_read``: buffer-pool hit/miss ratio
    - ``tup_inserted`` / ``tup_updated`` / ``tup_deleted``: DML row counts
    - ``deadlocks``: cumulative deadlock count (non-zero is a red flag)
    - ``sampled_at``: server timestamp of this sample

    Returns a JSON object.  All counter values are cumulative since the last
    ``pg_stat_reset()`` call.
    """
    sql = """
        SELECT
            datname,
            numbackends,
            xact_commit,
            xact_rollback,
            xact_commit + xact_rollback                              AS total_xacts,
            blks_hit,
            blks_read,
            CASE WHEN blks_hit + blks_read > 0
                 THEN ROUND(blks_hit::numeric / (blks_hit + blks_read) * 100, 2)
                 ELSE NULL
            END                                                      AS cache_hit_pct,
            tup_returned,
            tup_fetched,
            tup_inserted,
            tup_updated,
            tup_deleted,
            conflicts,
            deadlocks,
            NOW()                                                    AS sampled_at
        FROM pg_stat_database
        WHERE datname = current_database();
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            row = cur.fetchone()
    return _rows_to_json([dict(row)] if row else [])


# ---------------------------------------------------------------------------
# Tool: get_blocking_locks
# ---------------------------------------------------------------------------

@mcp.tool()
def get_blocking_locks() -> str:
    """
    Identify queries that are currently blocking other queries.

    Uses the canonical ``pg_locks ⟕ pg_stat_activity`` join to find lock
    conflicts.  Returns one row per (blocked_pid, blocker_pid) pair.

    Key columns:
    - ``blocked_pid``: PID of the query that is waiting
    - ``blocked_query``: the SQL text of the waiting query (truncated at 200 chars)
    - ``blocked_duration_seconds``: how long the blocked query has been waiting
    - ``blocker_pid``: PID of the query holding the conflicting lock
    - ``blocker_query``: the SQL text of the blocking query (truncated at 200 chars)
    - ``blocker_state``: e.g. ``idle in transaction`` — a common culprit
    - ``blocker_duration_seconds``: how long the blocker has been running

    Returns an empty JSON array ``[]`` if there are no lock conflicts.
    """
    sql = """
        SELECT
            blocked_activity.pid                                         AS blocked_pid,
            blocked_activity.usename                                     AS blocked_user,
            blocked_activity.application_name                           AS blocked_app,
            LEFT(blocked_activity.query, 200)                           AS blocked_query,
            blocked_activity.wait_event_type,
            blocked_activity.wait_event,
            EXTRACT(EPOCH FROM (NOW() - blocked_activity.query_start))::int
                                                                         AS blocked_duration_seconds,
            blocker_activity.pid                                         AS blocker_pid,
            blocker_activity.usename                                     AS blocker_user,
            blocker_activity.application_name                           AS blocker_app,
            LEFT(blocker_activity.query, 200)                           AS blocker_query,
            blocker_activity.state                                       AS blocker_state,
            EXTRACT(EPOCH FROM (NOW() - blocker_activity.query_start))::int
                                                                         AS blocker_duration_seconds,
            blocker_activity.client_addr                                 AS blocker_client
        FROM pg_stat_activity        AS blocked_activity
        JOIN pg_locks                AS blocked_locks
            ON blocked_activity.pid = blocked_locks.pid
        JOIN pg_locks                AS blocker_locks
            ON  blocked_locks.locktype             = blocker_locks.locktype
            AND blocked_locks.database  IS NOT DISTINCT FROM blocker_locks.database
            AND blocked_locks.relation  IS NOT DISTINCT FROM blocker_locks.relation
            AND blocked_locks.page      IS NOT DISTINCT FROM blocker_locks.page
            AND blocked_locks.tuple     IS NOT DISTINCT FROM blocker_locks.tuple
            AND blocked_locks.virtualxid IS NOT DISTINCT FROM blocker_locks.virtualxid
            AND blocked_locks.transactionid IS NOT DISTINCT FROM blocker_locks.transactionid
            AND blocked_locks.classid   IS NOT DISTINCT FROM blocker_locks.classid
            AND blocked_locks.objid     IS NOT DISTINCT FROM blocker_locks.objid
            AND blocked_locks.objsubid  IS NOT DISTINCT FROM blocker_locks.objsubid
            AND blocker_locks.pid != blocked_locks.pid
        JOIN pg_stat_activity        AS blocker_activity
            ON blocker_activity.pid = blocker_locks.pid
        WHERE NOT blocked_locks.granted
        ORDER BY blocker_duration_seconds DESC;
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return _rows_to_json([dict(r) for r in rows])


# ---------------------------------------------------------------------------
# Tool: get_active_queries
# ---------------------------------------------------------------------------

@mcp.tool()
def get_active_queries() -> str:
    """
    List all non-idle backend sessions with their current query and wait state.

    Excludes the MCP server's own connection (``pg_backend_pid()``).

    Key columns:
    - ``pid``: process ID
    - ``state``: ``active``, ``idle in transaction``, ``idle``, etc.
    - ``wait_event_type`` / ``wait_event``: e.g. ``Lock`` / ``relation``
    - ``query_duration``: how long the current query has been running
    - ``query_snippet``: first 200 characters of the query text

    Returns a JSON array, ordered by query start time (oldest first).
    """
    sql = """
        SELECT
            pid,
            usename,
            application_name,
            client_addr,
            state,
            wait_event_type,
            wait_event,
            query_start,
            EXTRACT(EPOCH FROM (NOW() - query_start))::int   AS query_duration_seconds,
            LEFT(query, 200)                                  AS query_snippet
        FROM pg_stat_activity
        WHERE state IS NOT NULL
          AND state <> 'idle'
          AND pid <> pg_backend_pid()
        ORDER BY query_start;
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return _rows_to_json([dict(r) for r in rows])


# ---------------------------------------------------------------------------
# Tool: get_table_lock_stats
# ---------------------------------------------------------------------------

@mcp.tool()
def get_table_lock_stats() -> str:
    """
    Return per-table lock information for all relations currently locked.

    Useful for understanding which tables are hot-spots for contention.

    Key columns:
    - ``table_name``: relation name (via ``regclass`` cast)
    - ``mode``: lock mode, e.g. ``ShareLock``, ``ExclusiveLock``, etc.
    - ``granted``: ``true`` if the lock is held; ``false`` if waiting
    - ``lock_count``: number of backends in this (table, mode, granted) group

    Returns a JSON array ordered by table name and lock mode.
    """
    sql = """
        SELECT
            l.relation::regclass::text                       AS table_name,
            l.mode,
            l.granted,
            COUNT(*)                                         AS lock_count,
            ARRAY_AGG(l.pid ORDER BY l.pid)                 AS pids
        FROM pg_locks l
        WHERE l.relation IS NOT NULL
          AND l.locktype = 'relation'
        GROUP BY l.relation, l.mode, l.granted
        ORDER BY table_name, mode;
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return _rows_to_json([dict(r) for r in rows])


# ---------------------------------------------------------------------------
# Tool: cancel_backend
# ---------------------------------------------------------------------------

@mcp.tool()
def cancel_backend(pid: int) -> str:
    """
    Send an interrupt signal to a PostgreSQL backend process.

    This calls ``pg_cancel_backend(pid)``, which asks the backend to cancel
    its **current query** gracefully.  The connection itself is kept alive and
    the client can issue a new query.

    This is *safer* than ``pg_terminate_backend``, which kills the connection
    entirely.  Use this tool to unblock stuck workloads caused by a long-
    running query holding locks.

    Parameters
    ----------
    pid : int
        The process ID (from ``pg_stat_activity.pid``) of the backend to cancel.

    Returns
    -------
    JSON object with:
    - ``pid``: the PID that was targeted
    - ``success``: ``true`` if pg_cancel_backend returned true
    - ``message``: human-readable outcome description

    Notes
    -----
    * Requires ``pg_signal_backend`` privilege (granted to superusers and
      roles with that privilege in PG 14+).
    * The backend may not respond immediately; the query is cancelled
      asynchronously.
    * The calling user must have the same role or be a superuser.
    """
    sql = "SELECT pg_cancel_backend(%s) AS cancelled;"
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (pid,))
            row = cur.fetchone()

    success = bool(row["cancelled"]) if row else False
    return json.dumps({
        "pid": pid,
        "success": success,
        "message": (
            f"pg_cancel_backend({pid}) succeeded — the query will be cancelled."
            if success else
            f"pg_cancel_backend({pid}) returned false — PID may no longer exist "
            "or you lack permission."
        ),
    })


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Run as an MCP server over stdio (the transport used by the agent).
    # The FastMCP.run() call blocks until stdin is closed.
    mcp.run(transport="stdio")
