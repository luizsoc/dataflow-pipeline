"""Load raw rows into the Bronze layer.

Idempotency: each file is identified by its SHA-256. A file whose hash is
already in bronze.ingestion_batches is never loaded again, so re-running the
pipeline does not duplicate data.
"""

from __future__ import annotations

import io
import logging
import uuid
from dataclasses import dataclass

import pandas as pd
from psycopg2.extensions import cursor as PgCursor  # noqa: N812

from dataflow.extraction.download import DownloadedFile
from dataflow.extraction.reader import BRONZE_COLUMNS
from dataflow.logging_config import kv

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BronzeLoadResult:
    batch_id: str
    rows_loaded: int
    skipped: bool

    def to_dict(self) -> dict[str, str | int | bool]:
        return {"batch_id": self.batch_id, "rows_loaded": self.rows_loaded, "skipped": self.skipped}


def find_batch_by_hash(cur: PgCursor, sha256: str) -> tuple[str, int] | None:
    """Return (batch_id, row_count) if this exact file was already ingested."""
    cur.execute(
        "SELECT batch_id::text, row_count FROM bronze.ingestion_batches WHERE file_sha256 = %s",
        (sha256,),
    )
    row = cur.fetchone()
    return (row[0], row[1]) if row else None


def _copy_rows(cur: PgCursor, batch_id: str, frame: pd.DataFrame) -> None:
    """Bulk load with COPY: much faster than row-by-row INSERTs for ~1M rows.

    `frame` must be a copy the caller does not reuse: a batch_id column is added to it.
    """
    buffer = io.StringIO()
    frame.insert(0, "batch_id", batch_id)
    # Missing values are written unquoted and empty, which COPY CSV reads as NULL
    frame.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    columns = ", ".join(["batch_id", *BRONZE_COLUMNS])
    cur.copy_expert(
        f"COPY bronze.online_retail_raw ({columns}) FROM STDIN WITH (FORMAT csv)",
        buffer,
    )


def load_bronze(cur: PgCursor, source: DownloadedFile, frame: pd.DataFrame) -> BronzeLoadResult:
    """Register the batch and copy its rows. The caller owns the transaction.

    Batch row and data rows are written in the same transaction, so a batch
    is either fully loaded or not loaded at all.
    """
    existing = find_batch_by_hash(cur, source.sha256)
    if existing:
        batch_id, row_count = existing
        logger.info(kv(step="load_bronze", event="already_loaded", batch_id=batch_id))
        return BronzeLoadResult(batch_id=batch_id, rows_loaded=row_count, skipped=True)

    missing = set(BRONZE_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"DataFrame is missing Bronze columns: {sorted(missing)}")

    batch_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO bronze.ingestion_batches
            (batch_id, source_url, file_name, file_sha256, row_count, extracted_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            batch_id,
            source.source_url,
            source.path.name,
            source.sha256,
            len(frame),
            source.extracted_at,
        ),
    )
    _copy_rows(cur, batch_id, frame[BRONZE_COLUMNS].copy())
    logger.info(kv(step="load_bronze", event="loaded", batch_id=batch_id, rows=len(frame)))
    return BronzeLoadResult(batch_id=batch_id, rows_loaded=len(frame), skipped=False)
