"""Bronze -> staging -> Silver against the real warehouse, inside one
transaction that the dw_conn fixture rolls back."""

from datetime import UTC, datetime

import pytest

from dataflow.extraction.download import DownloadedFile, file_sha256
from dataflow.extraction.reader import read_source
from dataflow.loading import (
    load_bronze,
    merge_staging_into_silver,
    read_bronze_batch,
    write_rejected,
    write_staging,
)
from dataflow.transformation import clean_sales
from tests.sample_data import TOTAL_ROWS, write_zip

pytestmark = pytest.mark.integration


@pytest.fixture
def bronze_batch(dw_conn, tmp_path) -> str:
    path = write_zip(tmp_path / "online_retail_ii.zip")
    source = DownloadedFile(
        path=path,
        sha256=file_sha256(path),
        size_bytes=path.stat().st_size,
        source_url="https://example.org/online_retail_ii.zip",
        extracted_at=datetime.now(UTC),
    )
    with dw_conn.cursor() as cur:
        result = load_bronze(cur, source, read_source(path))
    if result.skipped:
        pytest.skip("a committed batch with this hash already exists")
    return result.batch_id


def _process(cur, batch_id: str) -> tuple[int, int, int]:
    raw = read_bronze_batch(cur, batch_id)
    result = clean_sales(raw, batch_id)
    staged = write_staging(cur, result.clean)
    rejected = write_rejected(cur, result.rejected, batch_id)
    return len(raw), staged, rejected


def test_batch_flows_into_silver(dw_conn, bronze_batch) -> None:
    with dw_conn.cursor() as cur:
        rows_read, staged, rejected = _process(cur, bronze_batch)
        merge_staging_into_silver(cur)

        cur.execute(
            "SELECT count(*) FROM silver.sales_transactions WHERE batch_id = %s", (bronze_batch,)
        )
        in_silver = cur.fetchone()[0]
        cur.execute(
            "SELECT rejection_reason FROM silver.sales_rejected WHERE batch_id = %s",
            (bronze_batch,),
        )
        reasons = [row[0] for row in cur.fetchall()]

    assert rows_read == TOTAL_ROWS
    assert staged + rejected == TOTAL_ROWS
    assert in_silver == staged
    assert reasons == ["non_product_code"]  # the POST line in the sample


def test_line_total_is_computed_by_the_database(dw_conn, bronze_batch) -> None:
    with dw_conn.cursor() as cur:
        _process(cur, bronze_batch)
        merge_staging_into_silver(cur)
        cur.execute(
            """
            SELECT quantity, unit_price, line_total FROM silver.sales_transactions
            WHERE batch_id = %s AND invoice_no = 'C489449'
            """,
            (bronze_batch,),
        )
        quantity, unit_price, line_total = cur.fetchone()

    assert line_total == quantity * unit_price


def test_reprocessing_the_same_batch_is_idempotent(dw_conn, bronze_batch) -> None:
    with dw_conn.cursor() as cur:
        _process(cur, bronze_batch)
        merge_staging_into_silver(cur)
        _process(cur, bronze_batch)
        merge_staging_into_silver(cur)

        cur.execute(
            "SELECT count(*) FROM silver.sales_transactions WHERE batch_id = %s", (bronze_batch,)
        )
        in_silver = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*) FROM silver.sales_rejected WHERE batch_id = %s", (bronze_batch,)
        )
        in_quarantine = cur.fetchone()[0]

    assert in_silver + in_quarantine == TOTAL_ROWS
