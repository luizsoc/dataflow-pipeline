"""Checks that sql/init created the warehouse as designed.

Run with the stack up:  docker compose exec airflow-scheduler pytest -m integration
"""

import psycopg2
import pytest

pytestmark = pytest.mark.integration

EXPECTED_TABLES = {
    ("bronze", "ingestion_batches"),
    ("bronze", "online_retail_raw"),
    ("silver", "sales_staging"),
    ("silver", "sales_transactions"),
    ("silver", "sales_rejected"),
    ("gold", "dim_date"),
    ("gold", "dim_product"),
    ("gold", "dim_customer"),
    ("gold", "dim_country"),
    ("gold", "fact_sales"),
    ("audit", "pipeline_runs"),
    ("audit", "quality_checks"),
}


def test_all_layer_tables_exist(dw_conn) -> None:
    with dw_conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('bronze', 'silver', 'gold', 'audit')
            """
        )
        found = set(cur.fetchall())

    assert EXPECTED_TABLES <= found


def test_unknown_members_are_seeded(dw_conn) -> None:
    with dw_conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM gold.dim_customer WHERE customer_key = -1")
        assert cur.fetchone() == (None,)
        cur.execute("SELECT country_name FROM gold.dim_country WHERE country_key = -1")
        assert cur.fetchone() == ("Unknown",)


def test_silver_rejects_zero_quantity(dw_conn) -> None:
    with dw_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bronze.ingestion_batches
                (batch_id, source_url, file_name, file_sha256, row_count, extracted_at)
            VALUES (gen_random_uuid(), 'test', 'test.zip', repeat('0', 64), 0, now())
            RETURNING batch_id
            """
        )
        batch_id = cur.fetchone()[0]
        with pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute(
                """
                INSERT INTO silver.sales_transactions
                    (line_key, invoice_no, stock_code, quantity, unit_price,
                     invoice_ts, country, is_cancellation, batch_id)
                VALUES (repeat('a', 32), '489434', '85048', 0, 6.95,
                        '2009-12-01 07:45', 'United Kingdom', false, %s)
                """,
                (batch_id,),
            )
    # the dw_conn fixture rolls back, so the test batch is never persisted
