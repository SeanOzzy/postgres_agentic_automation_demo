"""
utils/config.py
===============
Configuration loading for the PostgreSQL Agentic Troubleshooting Demo.

Config is stored in YAML (config.yaml by default).  A ``config.local.yaml``
file in the same directory is merged on top, so developers can override
settings (e.g. passwords) without touching the committed file.

The config path can also be set via the ``PG_AGENT_CONFIG`` environment
variable, which is how the MCP server subprocess discovers it when the agent
launches it.

Usage
-----
    from utils.config import load_config

    cfg = load_config()
    print(cfg.database.host)
    print(cfg.monitoring.check_interval_seconds)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import yaml


# ---------------------------------------------------------------------------
# Dataclasses — one per top-level YAML section
# ---------------------------------------------------------------------------

@dataclass
class DatabaseConfig:
    """PostgreSQL connection parameters."""
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = ""
    dbname: str = "postgres"

    def dsn(self) -> str:
        """Return a libpq connection string (DSN) for this configuration."""
        parts = [
            f"host={self.host}",
            f"port={self.port}",
            f"user={self.user}",
            f"dbname={self.dbname}",
        ]
        if self.password:
            parts.append(f"password={self.password}")
        return " ".join(parts)

    def connection_kwargs(self) -> dict:
        """Return a dict suitable for **kwargs to psycopg2.connect()."""
        kwargs = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "dbname": self.dbname,
        }
        if self.password:
            kwargs["password"] = self.password
        return kwargs


@dataclass
class MonitoringConfig:
    """Parameters that govern the TPS-monitoring loop."""
    check_interval_seconds: int = 5
    baseline_samples: int = 6
    tps_drop_threshold: float = 0.5
    investigation_cooldown_seconds: int = 60


@dataclass
class AgentConfig:
    """Claude model + behaviour settings."""
    model: str = "claude-haiku-4-5-20251001"
    max_tokens: int = 4096
    max_investigation_turns: int = 10
    requires_confirmation: List[str] = field(
        default_factory=lambda: ["cancel_backend"]
    )


@dataclass
class WorkloadConfig:
    """pgbench workload parameters."""
    scale_factor: int = 10
    num_clients: int = 5
    num_threads: int = 2
    duration_seconds: int = 300
    progress_interval_seconds: int = 5


@dataclass
class AppConfig:
    """Root configuration object — aggregates all sections."""
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    agent: AgentConfig = field(default_factory=AgentConfig)
    workload: WorkloadConfig = field(default_factory=WorkloadConfig)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into *base* (non-destructive)."""
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(config_path: str | Path | None = None) -> AppConfig:
    """
    Load application configuration from YAML.

    Resolution order (later entries win):
    1. Compiled-in defaults (dataclass defaults above)
    2. ``config.yaml`` in the same directory as the config_path
    3. ``config.local.yaml`` in the same directory (for local overrides)

    Parameters
    ----------
    config_path:
        Path to the primary YAML config file.  Defaults to the value of the
        ``PG_AGENT_CONFIG`` environment variable, or ``config.yaml`` relative
        to this file's parent directory.

    Returns
    -------
    AppConfig
        Fully populated configuration object.
    """
    # Resolve the base config path
    if config_path is None:
        env_path = os.environ.get("PG_AGENT_CONFIG")
        if env_path:
            config_path = Path(env_path)
        else:
            # Default: config.yaml two levels up (project root)
            config_path = Path(__file__).parent.parent / "config.yaml"
    else:
        config_path = Path(config_path)

    # Load base config
    raw: dict = {}
    if config_path.exists():
        with open(config_path) as fh:
            raw = yaml.safe_load(fh) or {}

    # Merge local override (config.local.yaml alongside config.yaml)
    local_path = config_path.parent / "config.local.yaml"
    if local_path.exists():
        with open(local_path) as fh:
            local_raw = yaml.safe_load(fh) or {}
        raw = _deep_merge(raw, local_raw)

    # Populate dataclasses
    db_section = raw.get("database", {})
    mon_section = raw.get("monitoring", {})
    agent_section = raw.get("agent", {})
    wl_section = raw.get("workload", {})

    return AppConfig(
        database=DatabaseConfig(**{
            k: v for k, v in db_section.items()
            if k in DatabaseConfig.__dataclass_fields__
        }),
        monitoring=MonitoringConfig(**{
            k: v for k, v in mon_section.items()
            if k in MonitoringConfig.__dataclass_fields__
        }),
        agent=AgentConfig(**{
            k: v for k, v in agent_section.items()
            if k in AgentConfig.__dataclass_fields__
        }),
        workload=WorkloadConfig(**{
            k: v for k, v in wl_section.items()
            if k in WorkloadConfig.__dataclass_fields__
        }),
    )
