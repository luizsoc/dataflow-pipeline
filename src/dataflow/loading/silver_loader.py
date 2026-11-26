"""Move a Bronze batch through staging into Silver.

Flow: read Bronze batch -> clean in Pandas -> TRUNCATE + COPY into staging
-> upsert quarantine -> (quality checks, Delivery 4) -> merge staging into Silver.
"""

from __future__ import annotations

import io
import logging

import pandas as pd
from psycopg2.extensions import cursor as PgCursor  # noqa: N812
from psycopg2.extras import execute_values

from dataflow.database.sql_files import read_sql
from dataflow.extraction.reader import SOURCE_COLUMNS
from dataflow.logging_config import kv
from dataflow.transformation.cleaning import STAGING_COLUMNS

logger = logging.getLogger(__name__)

_RAW_COLUMNS = ["raw_id", *SOURCE_COLUMNS.values()]


def read_bronze_batch(cur: PgCursor, batch_id: str) -> pd.DataFrame:
    """Export one batch with COPY TO STDOUT, the fastest way out of PostgreSQL."""
    buffer = io.StringIO()
    cur.copy_expert(
        cur.mogrify(
            f"COPY (SELECT {', '.join(_RAW_COLUMNS)} FROM bronze.online_retail_raw "
            "WHERE batch_id = %s ORDER BY raw_id) TO STDOUT WITH (FORMAT csv, HEADER true)",
            (batch_id,),
        ).decode(),
        buffer,
    )
    buffer.seek(0)
    frame = pd.read_csv(buffer, dtype="string", keep_default_na=False, na_values=[""])
    logger.info(kv(step="read_bronze", batch_id=batch_id, rows=len(frame)))
    return frame


def write_staging(cur: PgCursor, clean: pd.DataFrame) -> int:
    """Replace the staging content with this batch (TRUNCATE makes it idempotent)."""
    cur.execute("TRUNCATE silver.sales_staging")
    buffer = io.StringIO()
    out = clean[STAGING_COLUMNS].copy()
    out["invoice_ts"] = out["invoice_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    out.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    cur.copy_expert(
        f"COPY silver.sales_staging ({', '.join(STAGING_COLUMNS)}) FROM STDIN WITH (FORMAT csv)",
        buffer,
    )
    logger.info(kv(step="write_staging", rows=len(clean)))
    return len(clean)


def write_rejected(cur: PgCursor, rejected: pd.DataFrame, batch_id: str) -> int:
    """Upsert quarantined rows: re-processing a batch updates reasons, never duplicates."""
    rows = [(int(r.raw_id), batch_id, r.rejection_reason) for r in rejected.itertuples()]
    if not rows:
        return 0
    execute_values(
        cur,
        """
        INSERT INTO silver.sales_rejected (raw_id, batch_id, rejection_reason)
        VALUES %s
        ON CONFLICT (raw_id) DO UPDATE SET
            rejection_reason = EXCLUDED.rejection_reason,
            rejected_at = now()
        """,
        rows,
        page_size=5000,
    )
    logger.info(kv(step="write_rejected", batch_id=batch_id, rows=len(rows)))
    return len(rows)


def merge_staging_into_silver(cur: PgCursor) -> int:
    cur.execute(read_sql("silver/merge_sales.sql"))
    logger.info(kv(step="load_silver", rows_changed=cur.rowcount))
    return cur.rowcount
