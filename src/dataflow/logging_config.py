"""Structured key=value logging shared by every pipeline module.

Example line:
    2026-11-25 23:10:00 INFO dataflow.loading step=load_bronze rows=1067371 batch_id=...
"""

from __future__ import annotations

import logging
from typing import Any

_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def configure_logging(level: str = "INFO") -> None:
    """Configure the root logger once. Inside Airflow, its own handlers are kept."""
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=level, format=_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
    root.setLevel(level)


def kv(**fields: Any) -> str:
    """Render fields as `key=value` pairs; values with spaces are quoted."""
    parts = []
    for key, value in fields.items():
        text = str(value)
        if " " in text:
            text = f'"{text}"'
        parts.append(f"{key}={text}")
    return " ".join(parts)
