"""Silver -> Gold: build the star schema with set-based SQL inside PostgreSQL.

Order matters: dimensions first, then the fact, which looks up their keys.
The caller runs everything in one transaction, so Gold is never half-built.
"""

from __future__ import annotations

import logging

from psycopg2.extensions import cursor as PgCursor  # noqa: N812

from dataflow.database.sql_files import read_sql
from dataflow.logging_config import kv

logger = logging.getLogger(__name__)

GOLD_STEPS: list[tuple[str, str]] = [
    ("dim_date", "gold/01_dim_date.sql"),
    ("dim_country", "gold/02_dim_country.sql"),
    ("dim_product", "gold/03_dim_product.sql"),
    ("dim_customer", "gold/04_dim_customer.sql"),
    ("fact_sales", "gold/05_fact_sales.sql"),
]


def build_gold(cur: PgCursor) -> dict[str, int]:
    """Run every Gold step and return rows inserted or changed per table."""
    changed: dict[str, int] = {}
    for table, sql_path in GOLD_STEPS:
        cur.execute(read_sql(sql_path))
        changed[table] = cur.rowcount
        logger.info(kv(step="build_gold", table=table, rows_changed=cur.rowcount))
    return changed
