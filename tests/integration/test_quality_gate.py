"""The quality gate against the real warehouse: clean data passes, invalid data
is stopped before Silver. Everything is rolled back by the dw_conn fixture."""

from datetime import UTC, datetime

import pytest

from dataflow.extraction.download import DownloadedFile, file_sha256
from dataflow.extraction.reader import read_source
from dataflow.loading import load_bronze, read_bronze_batch, write_rejected, write_staging
from dataflow.transformation import clean_sales
from dataflow.validation import (
    STAGING_CHECKS,
    DataQualityError,
    raise_on_critical_failures,
    record_results,
    run_checks,
)
from tests.sample_data import healthy_sheets, write_zip

pytestmark = pytest.mark.integration


@pytest.fixture
def staged_batch(dw_conn, tmp_path) -> str:
    """34-line batch loaded to Bronze, cleaned and staged: 1 rejected, 33 staged."""
    path = write_zip(tmp_path / "online_retail_ii.zip", healthy_sheets())
    source = DownloadedFile(
        path=path,
        sha256=file_sha256(path),
        size_bytes=path.stat().st_size,
        source_url="https://example.org/online_retail_ii.zip",
        extracted_at=datetime.now(UTC),
    )
    with dw_conn.cursor() as cur:
        bronze = load_bronze(cur, source, read_source(path))
        if bronze.skipped:
            pytest.skip("a committed batch with this hash already exists")
        result = clean_sales(read_bronze_batch(cur, bronze.batch_id), bronze.batch_id)
        write_staging(cur, result.clean)
        write_rejected(cur, result.rejected, bronze.batch_id)
    return bronze.batch_id


def failed(results) -> dict[str, int]:
    return {r.check.name: r.failed_rows for r in results if not r.passed}


def test_clean_batch_passes_every_critical_check(dw_conn, staged_batch) -> None:
    with dw_conn.cursor() as cur:
        results = run_checks(cur, STAGING_CHECKS, staged_batch)

    raise_on_critical_failures(results)  # must not raise


def test_invalid_rows_are_stopped_before_silver(dw_conn, staged_batch) -> None:
    with dw_conn.cursor() as cur:
        # Simulate a bug in the cleaning step: a zero price and a duplicated line slip through
        cur.execute(
            """
            INSERT INTO silver.sales_staging
            SELECT line_key, invoice_no, stock_code, description, quantity, 0,
                   invoice_ts, customer_id, country, is_cancellation, batch_id
            FROM silver.sales_staging LIMIT 1
            """
        )
        results = run_checks(cur, STAGING_CHECKS, staged_batch)

        with pytest.raises(DataQualityError):
            raise_on_critical_failures(results)

        cur.execute(
            "SELECT count(*) FROM silver.sales_transactions WHERE batch_id = %s", (staged_batch,)
        )
        assert cur.fetchone()[0] == 0  # nothing reached Silver

    assert failed(results) == {"line_key_unique": 1, "unit_price_positive": 1}


def test_high_rejection_rate_is_critical(dw_conn, staged_batch) -> None:
    with dw_conn.cursor() as cur:
        # Lose most staged rows: 1 rejected out of 4 is a 25% rejection rate
        cur.execute(
            "DELETE FROM silver.sales_staging WHERE ctid IN "
            "(SELECT ctid FROM silver.sales_staging LIMIT 30)"
        )
        results = run_checks(cur, STAGING_CHECKS, staged_batch)

    assert "rejection_rate_below_10pct" in failed(results)


def test_results_are_recorded_in_audit(dw_conn, staged_batch) -> None:
    with dw_conn.cursor() as cur:
        results = run_checks(cur, STAGING_CHECKS, staged_batch)
        record_results(cur, results, dag_run_id="test-run", batch_id=staged_batch)

        cur.execute(
            "SELECT count(*), count(*) FILTER (WHERE passed) FROM audit.quality_checks "
            "WHERE dag_run_id = 'test-run'"
        )
        total, passed = cur.fetchone()

    assert total == len(STAGING_CHECKS)
    assert passed == sum(r.passed for r in results)
