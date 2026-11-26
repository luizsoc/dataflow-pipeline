"""Bronze load against the real warehouse. Everything runs in one transaction
that the dw_conn fixture rolls back, so the warehouse is left untouched."""

from datetime import UTC, datetime

import pytest

from dataflow.extraction.download import DownloadedFile, file_sha256
from dataflow.extraction.reader import read_source
from dataflow.loading.bronze_loader import load_bronze
from tests.sample_data import TOTAL_ROWS, write_zip

pytestmark = pytest.mark.integration


@pytest.fixture
def source(tmp_path) -> DownloadedFile:
    path = write_zip(tmp_path / "online_retail_ii.zip")
    return DownloadedFile(
        path=path,
        sha256=file_sha256(path),
        size_bytes=path.stat().st_size,
        source_url="https://example.org/online_retail_ii.zip",
        extracted_at=datetime.now(UTC),
    )


def _cleanup_same_hash(cur, sha256: str) -> None:
    """The test file is deterministic only per run; guard against a leftover batch."""
    cur.execute(
        "SELECT 1 FROM bronze.ingestion_batches WHERE file_sha256 = %s",
        (sha256,),
    )
    if cur.fetchone():
        pytest.skip("a committed batch with this hash already exists")


def test_load_registers_batch_and_rows(dw_conn, source) -> None:
    frame = read_source(source.path)
    with dw_conn.cursor() as cur:
        _cleanup_same_hash(cur, source.sha256)

        result = load_bronze(cur, source, frame)

        cur.execute(
            "SELECT count(*) FROM bronze.online_retail_raw WHERE batch_id = %s",
            (result.batch_id,),
        )
        assert cur.fetchone()[0] == TOTAL_ROWS
        cur.execute(
            "SELECT row_count, file_sha256 FROM bronze.ingestion_batches WHERE batch_id = %s",
            (result.batch_id,),
        )
        assert cur.fetchone() == (TOTAL_ROWS, source.sha256)
    assert result.skipped is False


def test_missing_values_are_stored_as_null(dw_conn, source) -> None:
    frame = read_source(source.path)
    with dw_conn.cursor() as cur:
        _cleanup_same_hash(cur, source.sha256)
        result = load_bronze(cur, source, frame)

        cur.execute(
            """
            SELECT customer_id, description FROM bronze.online_retail_raw
            WHERE batch_id = %s AND invoice = 'C489449'
            """,
            (result.batch_id,),
        )
        assert cur.fetchone() == (None, None)


def test_same_file_twice_is_skipped(dw_conn, source) -> None:
    frame = read_source(source.path)
    with dw_conn.cursor() as cur:
        _cleanup_same_hash(cur, source.sha256)
        first = load_bronze(cur, source, frame)

        second = load_bronze(cur, source, frame)

        cur.execute(
            "SELECT count(*) FROM bronze.online_retail_raw WHERE batch_id = %s",
            (first.batch_id,),
        )
        assert cur.fetchone()[0] == TOTAL_ROWS
    assert second.skipped is True
    assert second.batch_id == first.batch_id
