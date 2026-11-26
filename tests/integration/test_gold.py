"""Silver -> Gold star schema and the analytical queries, against the real
warehouse. Rolled back by the dw_conn fixture."""

from datetime import UTC, datetime
from pathlib import Path

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
from dataflow.transformation import build_gold, clean_sales
from dataflow.validation import GOLD_CHECKS, raise_on_critical_failures, run_checks
from tests.sample_data import healthy_sheets, write_zip

pytestmark = pytest.mark.integration

ANALYTICS_DIR = Path(__file__).resolve().parents[2] / "sql" / "analytics"


@pytest.fixture
def gold_built(dw_conn, tmp_path) -> str:
    """Sample batch taken all the way to Gold."""
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
        cur.execute("SELECT count(*) FROM silver.sales_transactions")
        if cur.fetchone()[0]:
            pytest.skip("warehouse already holds data; Gold checks compare whole tables")
        result = clean_sales(read_bronze_batch(cur, bronze.batch_id), bronze.batch_id)
        write_staging(cur, result.clean)
        write_rejected(cur, result.rejected, bronze.batch_id)
        merge_staging_into_silver(cur)
        build_gold(cur)
    return bronze.batch_id


def scalar(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()[0]


def test_fact_matches_silver(dw_conn, gold_built) -> None:
    with dw_conn.cursor() as cur:
        results = run_checks(cur, GOLD_CHECKS, gold_built)
        assert scalar(cur, "SELECT count(*) FROM gold.fact_sales") == scalar(
            cur, "SELECT count(*) FROM silver.sales_transactions"
        )

    raise_on_critical_failures(results)


def test_sale_without_customer_points_to_unknown_member(dw_conn, gold_built) -> None:
    with dw_conn.cursor() as cur:
        customer_key = scalar(
            cur, "SELECT customer_key FROM gold.fact_sales WHERE invoice_no = 'C489449'"
        )
    assert customer_key == -1


def test_country_names_are_cleaned_in_the_dimension(dw_conn, gold_built) -> None:
    with dw_conn.cursor() as cur:
        cur.execute("SELECT country_name FROM gold.dim_country ORDER BY country_name")
        countries = [row[0] for row in cur.fetchall()]
    assert "Ireland" in countries
    assert "EIRE" not in countries


def test_date_dimension_attributes(dw_conn, gold_built) -> None:
    with dw_conn.cursor() as cur:
        cur.execute(
            "SELECT year, quarter, month_name, day_of_week, is_weekend "
            "FROM gold.dim_date WHERE date_key = 20091201"
        )
        row = cur.fetchone()
    assert row == (2009, 4, "December", 2, False)  # 2009-12-01 was a Tuesday


def test_rebuilding_gold_changes_nothing(dw_conn, gold_built) -> None:
    with dw_conn.cursor() as cur:
        before = scalar(cur, "SELECT count(*) FROM gold.fact_sales")
        changed = build_gold(cur)
        after = scalar(cur, "SELECT count(*) FROM gold.fact_sales")

    assert after == before
    assert changed == {
        "dim_date": 0,
        "dim_country": 0,
        "dim_product": 0,
        "dim_customer": 0,
        "fact_sales": 0,
    }


@pytest.mark.parametrize("query_file", sorted(p.name for p in ANALYTICS_DIR.glob("*.sql")))
def test_analytical_queries_run_and_return_rows(dw_conn, gold_built, query_file: str) -> None:
    with dw_conn.cursor() as cur:
        cur.execute((ANALYTICS_DIR / query_file).read_text(encoding="utf-8"))
        rows = cur.fetchall()
    # The cancellation query keeps only countries with 100+ invoices: none in the sample
    if query_file.startswith("03_"):
        assert rows == []
    else:
        assert rows
