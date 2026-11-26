"""Data quality checks: plain SQL queries with a severity.

Each check's query returns the number of failing rows (0 = pass). Critical
failures stop the pipeline; warnings are recorded and the pipeline continues.
Every result, pass or fail, is stored in audit.quality_checks.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum

from psycopg2.extensions import cursor as PgCursor  # noqa: N812

from dataflow.logging_config import kv

logger = logging.getLogger(__name__)

# First day of the Online Retail II dataset; anything earlier is a parsing error
DATASET_START = "2009-12-01"


class Severity(StrEnum):
    CRITICAL = "critical"
    WARNING = "warning"


@dataclass(frozen=True)
class QualityCheck:
    name: str
    layer: str
    severity: Severity
    description: str
    # Returns one integer: the number of failing rows. May use %(batch_id)s.
    sql: str


@dataclass(frozen=True)
class CheckResult:
    check: QualityCheck
    failed_rows: int

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0


class DataQualityError(RuntimeError):
    """Raised when at least one critical check fails."""

    def __init__(self, failures: list[CheckResult]) -> None:
        names = ", ".join(f"{r.check.name} ({r.failed_rows} rows)" for r in failures)
        super().__init__(f"Critical data quality checks failed: {names}")
        self.failures = failures


STAGING_CHECKS: list[QualityCheck] = [
    QualityCheck(
        name="staging_not_empty",
        layer="staging",
        severity=Severity.CRITICAL,
        description="The batch produced at least one clean row",
        sql="SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM silver.sales_staging",
    ),
    QualityCheck(
        name="required_fields_not_null",
        layer="staging",
        severity=Severity.CRITICAL,
        description="Invoice, product, quantity, price, date and country are filled",
        sql="""
            SELECT count(*) FROM silver.sales_staging
            WHERE invoice_no IS NULL OR stock_code IS NULL OR quantity IS NULL
               OR unit_price IS NULL OR invoice_ts IS NULL OR country IS NULL
        """,
    ),
    QualityCheck(
        name="line_key_unique",
        layer="staging",
        severity=Severity.CRITICAL,
        description="No duplicate sales lines",
        sql="SELECT count(*) - count(DISTINCT line_key) FROM silver.sales_staging",
    ),
    QualityCheck(
        name="invoice_date_in_range",
        layer="staging",
        severity=Severity.CRITICAL,
        description=f"Dates between {DATASET_START} and now",
        sql=f"""
            SELECT count(*) FROM silver.sales_staging
            WHERE invoice_ts < DATE '{DATASET_START}' OR invoice_ts > now()
        """,
    ),
    QualityCheck(
        name="unit_price_positive",
        layer="staging",
        severity=Severity.CRITICAL,
        description="Unit price greater than zero",
        sql="SELECT count(*) FROM silver.sales_staging WHERE unit_price <= 0",
    ),
    QualityCheck(
        name="quantity_not_zero",
        layer="staging",
        severity=Severity.CRITICAL,
        description="Quantity different from zero",
        sql="SELECT count(*) FROM silver.sales_staging WHERE quantity = 0",
    ),
    QualityCheck(
        name="rejection_rate_below_10pct",
        layer="staging",
        severity=Severity.CRITICAL,
        description="Less than 10% of the batch quarantined; more suggests a broken source",
        sql="""
            WITH totals AS (
                SELECT
                    (SELECT count(*) FROM silver.sales_staging) AS staged,
                    (SELECT count(*) FROM silver.sales_rejected
                     WHERE batch_id = %(batch_id)s) AS rejected
            )
            SELECT CASE WHEN rejected > 0.10 * (staged + rejected) THEN rejected ELSE 0 END
            FROM totals
        """,
    ),
    QualityCheck(
        name="cancellation_sign_consistent",
        layer="staging",
        severity=Severity.WARNING,
        description="Cancellations have negative quantity; sales have positive quantity",
        sql="""
            SELECT count(*) FROM silver.sales_staging
            WHERE (is_cancellation AND quantity > 0) OR (NOT is_cancellation AND quantity < 0)
        """,
    ),
    QualityCheck(
        name="missing_customer_below_30pct",
        layer="staging",
        severity=Severity.WARNING,
        description="Less than 30% of lines without a customer",
        sql="""
            SELECT CASE WHEN count(*) FILTER (WHERE customer_id IS NULL) > 0.30 * count(*)
                        THEN count(*) FILTER (WHERE customer_id IS NULL) ELSE 0 END
            FROM silver.sales_staging
        """,
    ),
]


GOLD_CHECKS: list[QualityCheck] = [
    QualityCheck(
        name="every_silver_line_in_fact",
        layer="gold",
        severity=Severity.CRITICAL,
        description="Each Silver line has its row in fact_sales",
        sql="""
            SELECT count(*) FROM silver.sales_transactions AS s
            WHERE NOT EXISTS (SELECT 1 FROM gold.fact_sales AS f WHERE f.line_key = s.line_key)
        """,
    ),
    QualityCheck(
        name="row_count_matches_silver",
        layer="gold",
        severity=Severity.CRITICAL,
        description="fact_sales and Silver have the same number of lines",
        sql="""
            SELECT abs((SELECT count(*) FROM gold.fact_sales)
                     - (SELECT count(*) FROM silver.sales_transactions))
        """,
    ),
    QualityCheck(
        name="revenue_matches_silver",
        layer="gold",
        severity=Severity.CRITICAL,
        description="Total line_total is identical in Silver and Gold",
        sql="""
            SELECT CASE WHEN (SELECT COALESCE(sum(line_total), 0) FROM gold.fact_sales)
                           = (SELECT COALESCE(sum(line_total), 0) FROM silver.sales_transactions)
                        THEN 0 ELSE 1 END
        """,
    ),
    QualityCheck(
        name="identified_customers_resolved",
        layer="gold",
        severity=Severity.CRITICAL,
        description="Lines with a customer id never point to the unknown customer",
        sql="""
            SELECT count(*) FROM gold.fact_sales AS f
            JOIN silver.sales_transactions AS s ON s.line_key = f.line_key
            WHERE s.customer_id IS NOT NULL AND f.customer_key = -1
        """,
    ),
]


def run_checks(cur: PgCursor, checks: list[QualityCheck], batch_id: str) -> list[CheckResult]:
    results = []
    for check in checks:
        cur.execute(check.sql, {"batch_id": batch_id})
        failed = int(cur.fetchone()[0])
        result = CheckResult(check=check, failed_rows=failed)
        results.append(result)
        log = logger.info if result.passed else logger.warning
        log(
            kv(
                step="quality",
                check=check.name,
                severity=check.severity.value,
                passed=result.passed,
                failed_rows=failed,
            )
        )
    return results


def record_results(
    cur: PgCursor, results: list[CheckResult], dag_run_id: str, batch_id: str
) -> None:
    cur.executemany(
        """
        INSERT INTO audit.quality_checks
            (dag_run_id, batch_id, check_name, layer, severity, passed, failed_rows, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [
            (
                dag_run_id,
                batch_id,
                r.check.name,
                r.check.layer,
                r.check.severity.value,
                r.passed,
                r.failed_rows,
                r.check.description,
            )
            for r in results
        ],
    )


def raise_on_critical_failures(results: list[CheckResult]) -> None:
    failures = [r for r in results if not r.passed and r.check.severity is Severity.CRITICAL]
    if failures:
        raise DataQualityError(failures)
