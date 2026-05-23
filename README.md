# 🐘 PostgreSQL Agentic Troubleshooter

> An AI-powered agent that detects, diagnoses, and resolves PostgreSQL lock contention — live — using Claude AI and the Model Context Protocol (MCP).

---

## What This Demo Does

This project demonstrates a **real agentic AI loop** applied to a genuine database problem:

1. A TPC-B-like **pgbench workload** hammers a local PostgreSQL instance.
2. A second connection executes `SELECT * FROM pgbench_accounts FOR UPDATE`, grabbing row-level locks on the entire table and **blocking every TPC-B transaction**.
3. An **AI agent** watches the database:
   - Detects the TPS drop via a Python monitoring loop
   - Invokes **Claude Haiku** to investigate via MCP tools
   - Identifies the blocking PID and its query
   - **Asks the operator for permission** before cancelling anything
   - Calls `pg_cancel_backend(pid)` and confirms TPS recovery

This is not a toy — it uses the same PostgreSQL diagnostic queries a senior DBA would reach for, wrapped as MCP tools that the LLM can call autonomously.

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    main.py (CLI)                          │
│   setup │ workload │ blocker │ agent │ demo               │
└────┬──────────┬─────────────┬────────┬────────────────────┘
     │          │             │        │
     │          ▼             ▼        ▼
     │   ┌──────────┐  ┌──────────┐  ┌───────────────────────┐
     │   │ pgbench  │  │ blocker  │  │  TroubleshootAgent    │
     │   │ TPC-B    │  │ SELECT * │  │                       │
     │   │ workload │  │ FOR UPD. │  │  ┌─────────────────┐  │
     │   └──────────┘  └──────────┘  │  │ Monitoring Loop │  │
     │        │              │       │  │  pg_stat_db TPS │  │
     │        │  (lock       │       │  └────────┬────────┘  │
     │        │   conflict)  │       │           │ anomaly   │
     │        └──────────────┘       │  ┌────────▼────────┐  │
     │                               │  │ Investigation   │  │
     │                               │  │ Claude Haiku    │  │
     │                               │  └────────┬────────┘  │
     │                               │           │ tool_use  │
     │                               │  ┌────────▼────────┐  │
     │                               │  │ MCP Dispatcher  │  │
     │                               │  └────────┬────────┘  │
     └───────────────────────────────┘           │
                                                 ▼
                              ┌──────────────────────────────┐
                              │  pg_diagnostic_server.py     │
                              │  (FastMCP subprocess, stdio) │
                              │                              │
                              │  get_database_metrics        │
                              │  get_blocking_locks          │
                              │  get_active_queries          │
                              │  get_table_lock_stats        │
                              │  cancel_backend ──► ✋ human │
                              └──────────────────────────────┘
                                            │
                                            ▼
                                    PostgreSQL (localhost)
```

### Key Design Decisions

| Decision | Rationale |
|---|---|
| **MCP for tools** | Tools are reusable across any agent; the server can be tested independently |
| **Monitoring in Python, not the LLM** | The LLM is expensive; simple threshold math belongs in code |
| **Claude Haiku** | Fast and cheap for structured reasoning over JSON tool output |
| **Human-in-the-loop for `cancel_backend`** | Destructive actions need operator approval; the agent pauses and prompts |
| **stdio transport** | No network ports needed; the MCP server is a child process |
| **psycopg2 (sync)** | Straightforward for demo; swap for `asyncpg` in production |
| **`config.yaml` + `config.local.yaml`** | Committed defaults; local secrets never reach git |

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.11+ | Tested on 3.11 and 3.12 |
| PostgreSQL 14+ | Local instance, any auth method |
| `pgbench` | Usually ships with PostgreSQL client packages |
| Anthropic API key | [console.anthropic.com](https://console.anthropic.com/) |

### Install pgbench

```bash
# Debian / Ubuntu
sudo apt install postgresql-client

# RHEL / Fedora / Rocky
sudo dnf install postgresql

# macOS (Homebrew)
brew install postgresql@16

# macOS (Postgres.app) — add to PATH
export PATH="/Applications/Postgres.app/Contents/Versions/latest/bin:$PATH"
```

---

## Installation

```bash
# Clone the repo
git clone https://github.com/your-username/postgres_agenticai_demo.git
cd postgres_agenticai_demo

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set your Anthropic API key
export ANTHROPIC_API_KEY=sk-ant-...
```

---

## Configuration

Edit `config.yaml` (or create `config.local.yaml` for local overrides):

```yaml
database:
  host: localhost
  port: 5432
  user: postgres
  password: ""       # leave empty for peer/trust auth
  dbname: postgres

monitoring:
  check_interval_seconds: 5     # how often to sample TPS
  baseline_samples: 6           # samples × interval = warm-up window
  tps_drop_threshold: 0.5       # alert if TPS < 50% of baseline
  investigation_cooldown_seconds: 60

agent:
  model: "claude-haiku-4-5-20251001"
  max_tokens: 4096
  max_investigation_turns: 10
  requires_confirmation:
    - cancel_backend             # always ask before cancelling

workload:
  scale_factor: 10              # 10 ≈ 1M rows, ~100 MB
  num_clients: 5
  num_threads: 2
  duration_seconds: 300
```

---

## Running the Demo

### Option A — Four Terminals

> **Order matters:** the agent must establish its TPS baseline *before* the
> blocker is introduced.  Start the agent first, wait for the
> `Baseline established` log line, *then* run the blocker.

```bash
# Terminal 0 — one-time setup
python main.py setup

# Terminal 1 — start the workload and leave it running
python main.py workload

# Terminal 2 — start the agent IMMEDIATELY after the workload
#              Wait for: "Baseline established: NNN TPS"
python main.py agent

# Terminal 3 — ONLY after baseline is logged, introduce the blocker
python main.py blocker
```

### Option B — Single Terminal (automated)

```bash
# Runs setup, workload, blocker, and agent automatically
python main.py demo --setup
```

---

## Expected Output

### Terminal 1 (workload)
```
progress:  5.0 s, 312.4 tps, lat 16.0 ms stddev 5.1
progress: 10.0 s, 298.7 tps, lat 16.7 ms stddev 4.8
progress: 15.0 s, 0.0 tps, lat 0.0 ms stddev 0.0    ← blocker is active
```

### Terminal 2 (blocker)
```
╭─ Blocker Script ─────────────────────────────────────╮
│ Executing: SELECT * FROM pgbench_accounts FOR UPDATE  │
│ Locks held. TPC-B workload should now be blocked.     │
╰──────────────────────────────────────────────────────╯
...
Query was cancelled by pg_cancel_backend.
The AI agent successfully identified and resolved the lock conflict!
```

### Terminal 3 (agent)
```
Baseline established: 305.3 TPS (avg of last 6 samples)
TPS:    1.2  (baseline: 305.3,  backends: 6)

╭── ⚠  Alert ────────────────────────────────╮
│ TPS ANOMALY DETECTED                        │
│   Current TPS :   1.2                       │
│   Baseline    : 305.3                       │
│   Drop        :  99.6%                      │
╰─────────────────────────────────────────────╯

Starting AI investigation...
  Tool call: get_blocking_locks()
  Tool call: get_active_queries()

╭── Agent Analysis ───────────────────────────────────────────────────────────╮
│ ## Root Cause Analysis                                                       │
│                                                                              │
│ The TPS drop is caused by a lock conflict:                                   │
│                                                                              │
│ **Blocker PID: 12345** (state: `idle in transaction`, running 47s)           │
│   Query: `SELECT * FROM pgbench_accounts FOR UPDATE`                         │
│                                                                              │
│ This query holds row-level locks on every row in `pgbench_accounts`.         │
│ All 5 pgbench clients are waiting for `RowShareLock` on this table.          │
│                                                                              │
│ **Recommendation**: Cancel PID 12345 to release the locks.                  │
╰─────────────────────────────────────────────────────────────────────────────╯

  Tool call: cancel_backend({'pid': 12345})

⚠  Action required
The agent wants to call cancel_backend(pid=12345).

Proceed with pg_cancel_backend(12345)? [y/N]: y

Confirmed. Executing cancel_backend({'pid': 12345})...
Investigation complete.
```

---

## Project Structure

```
postgres_agenticai_demo/
│
├── main.py                          # CLI entry point (setup/workload/blocker/agent/demo)
├── config.yaml                      # Default configuration
├── requirements.txt
├── .gitignore
│
├── utils/
│   ├── __init__.py
│   └── config.py                    # YAML → dataclasses config loader
│
├── mcp_server/
│   ├── __init__.py
│   └── pg_diagnostic_server.py      # FastMCP server — PostgreSQL diagnostic tools
│
├── agent/
│   ├── __init__.py
│   └── troubleshoot_agent.py        # TPS monitor + Claude Haiku investigation loop
│
└── workload/
    ├── __init__.py
    ├── pgbench_runner.py             # pgbench init and TPC-B workload runner
    └── blocker.py                    # Blocking SELECT ... FOR UPDATE script
```

---

## MCP Tools Reference

The MCP server (`mcp_server/pg_diagnostic_server.py`) exposes these tools to the agent:

| Tool | Description |
|---|---|
| `get_database_metrics` | Cumulative TPS counters, connection count, cache hit %, DML row counts, deadlocks |
| `get_blocking_locks` | Lock conflict tree — who is blocking whom, for how long, with query text |
| `get_active_queries` | All non-idle backends with state, wait event, duration, query snippet |
| `get_table_lock_stats` | Per-table lock mode and grant counts — identifies hot-spot tables |
| `cancel_backend(pid)` | Calls `pg_cancel_backend(pid)` — gated by human confirmation in the agent |

The server runs as a stdio subprocess and communicates via the MCP wire protocol.  It can be tested independently:

```bash
# List available tools (requires mcp CLI or any MCP client)
PG_AGENT_CONFIG=config.yaml python mcp_server/pg_diagnostic_server.py
```

---

## Extending the Agent

### Add a new diagnostic tool

1. Add a function decorated with `@mcp.tool()` in `mcp_server/pg_diagnostic_server.py`.
2. The agent automatically discovers it on startup — no changes needed elsewhere.

```python
@mcp.tool()
def get_long_running_queries(min_duration_seconds: int = 60) -> str:
    """Return queries that have been running longer than min_duration_seconds."""
    sql = """
        SELECT pid, usename, query_start,
               EXTRACT(EPOCH FROM (NOW() - query_start))::int AS duration_seconds,
               LEFT(query, 200) AS query_snippet
        FROM pg_stat_activity
        WHERE state = 'active'
          AND EXTRACT(EPOCH FROM (NOW() - query_start)) > %s
        ORDER BY query_start;
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (min_duration_seconds,))
            rows = cur.fetchall()
    return _rows_to_json([dict(r) for r in rows])
```

### Change the LLM model

Update `config.yaml`:

```yaml
agent:
  model: "claude-sonnet-4-5"   # More capable, higher cost
```

### Add a new anomaly type

Extend the `_monitoring_loop` in `agent/troubleshoot_agent.py` to detect other signals (e.g., dead-lock count, cache-hit-ratio drop, replication lag) and build a richer `InvestigationContext`.

### Use a remote PostgreSQL server

Update `config.yaml` (or `config.local.yaml`):

```yaml
database:
  host: db.example.com
  port: 5432
  user: monitoring_user
  password: "..."
  dbname: production
```

---

## Security Notes

* The `cancel_backend` tool requires the connected PostgreSQL role to have `pg_signal_backend` privilege (or be a superuser).
* `config.local.yaml` is `.gitignore`'d — put credentials there, not in `config.yaml`.
* The agent never executes DDL or destructive DML — it only reads system views and calls `pg_cancel_backend`.
* The human-confirmation gate (`requires_confirmation` in config) can be extended to cover `pg_terminate_backend` or any future tools.

---

## License

MIT — see [LICENSE](LICENSE).

---

## Author

Built as a demonstration of agentic AI applied to real infrastructure problems.  
Feedback and PRs welcome.
