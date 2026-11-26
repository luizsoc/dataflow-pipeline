"""Load SQL statements kept in the project's sql/ folder."""

from __future__ import annotations

import os
from functools import cache
from pathlib import Path

# sql/ sits next to src/ in the repo, and at /opt/airflow/sql in the container
_DEFAULT_SQL_DIR = Path(__file__).resolve().parents[3] / "sql"


@cache
def read_sql(relative_path: str) -> str:
    sql_dir = Path(os.environ.get("SQL_DIR", _DEFAULT_SQL_DIR))
    return (sql_dir / relative_path).read_text(encoding="utf-8")
