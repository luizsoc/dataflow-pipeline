"""Settings read from environment variables.

Nothing sensitive has a default: a missing password fails fast with a clear
message instead of silently connecting with a guessed value.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_SOURCE_URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"


class ConfigError(RuntimeError):
    """Raised when a required environment variable is missing or invalid."""


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class WarehouseSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> WarehouseSettings:
        port_raw = os.environ.get("DW_PORT", "5432")
        try:
            port = int(port_raw)
        except ValueError as exc:
            raise ConfigError(f"DW_PORT must be an integer, got {port_raw!r}") from exc
        return cls(
            host=os.environ.get("DW_HOST", "localhost"),
            port=port,
            dbname=_require("DW_DB"),
            user=_require("DW_USER"),
            password=_require("DW_PASSWORD"),
        )

    def __repr__(self) -> str:  # never print the password in logs
        return (
            f"WarehouseSettings(host={self.host!r}, port={self.port}, "
            f"dbname={self.dbname!r}, user={self.user!r}, password='***')"
        )


@dataclass(frozen=True)
class PipelineSettings:
    source_url: str
    data_dir: Path
    log_level: str

    @classmethod
    def from_env(cls) -> PipelineSettings:
        return cls(
            source_url=os.environ.get("SOURCE_URL") or DEFAULT_SOURCE_URL,
            data_dir=Path(os.environ.get("DATA_DIR", "data")),
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        )

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"
