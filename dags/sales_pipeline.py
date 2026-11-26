"""Sales pipeline: UCI Online Retail II -> Bronze -> Silver -> Gold.

The DAG only orchestrates. All logic lives in the `dataflow` package so it can
be tested without Airflow. Tasks pass small metadata between them via XCom
(paths, hashes, batch ids), never the data itself.

Flow:
    extract_source >> load_to_bronze >> transform_to_staging >> validate_staging
    >> load_silver >> build_gold >> validate_gold

validate_staging is the quality gate: a critical failure fails the task, and
load_silver (default trigger rule all_success) never runs, so Silver is untouched.

Every task execution, successful or failed, is recorded in audit.pipeline_runs
by the callbacks below. Errors that cannot fix themselves (a 404, a failed
quality check) fail immediately instead of wasting retries.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from dataflow.config import PipelineSettings
from dataflow.database import get_connection
from dataflow.database.audit import TaskRun, record_task_run, task_run_from_result
from dataflow.extraction import DownloadedFile, ExtractionError, download_source, read_source
from dataflow.loading import (
    BronzeLoadResult,
    find_batch_by_hash,
    load_bronze,
    merge_staging_into_silver,
    read_bronze_batch,
    write_rejected,
    write_staging,
)
from dataflow.logging_config import kv
from dataflow.transformation import build_gold, clean_sales
from dataflow.validation import (
    GOLD_CHECKS,
    STAGING_CHECKS,
    DataQualityError,
    raise_on_critical_failures,
    record_results,
    run_checks,
)

logger = logging.getLogger(__name__)


def _record_success(context: dict) -> None:
    ti = context["ti"]
    result = ti.xcom_pull(task_ids=ti.task_id)
    record_task_run(
        task_run_from_result(context["run_id"], ti.task_id, result, started_at=ti.start_date)
    )


def _record_failure(context: dict) -> None:
    ti = context["ti"]
    error = context.get("exception")
    record_task_run(
        TaskRun(
            dag_run_id=context["run_id"],
            task_id=ti.task_id,
            status="failed",
            error_message=f"{type(error).__name__}: {error}" if error else None,
            started_at=ti.start_date,
        )
    )


DEFAULT_ARGS = {
    "owner": "dataflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # A stuck task fails instead of blocking the next daily run (max_active_runs=1)
    "execution_timeout": timedelta(minutes=30),
    "on_success_callback": _record_success,
    "on_failure_callback": _record_failure,
}


@dag(
    dag_id="sales_pipeline",
    description="Retail sales ELT with Medallion Architecture (Bronze, Silver, Gold)",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # runs never write to the same tables at the same time
    default_args=DEFAULT_ARGS,
    tags=["dataflow", "sales", "medallion"],
    doc_md=__doc__,
    params={
        # Optional override, e.g. a mirror of the dataset; empty = SOURCE_URL from .env
        "source_url": Param(None, type=["null", "string"], description="Override the source URL"),
    },
)
def sales_pipeline() -> None:
    @task(retries=2, retry_delay=timedelta(minutes=5))  # network errors are often transient
    def extract_source() -> dict:
        settings = PipelineSettings.from_env()
        url = get_current_context()["params"].get("source_url") or settings.source_url
        try:
            downloaded = download_source(
                url=url,
                raw_dir=settings.raw_dir,
                ingestion_date=datetime.now(UTC).date(),
            )
        except ExtractionError as exc:
            if not exc.retryable:
                raise AirflowFailException(str(exc)) from exc
            raise  # transient: Airflow retries it
        return downloaded.to_dict()

    @task
    def load_to_bronze(source_info: dict) -> dict:
        source = DownloadedFile.from_dict(source_info)

        # Cheap check first: skip parsing ~1M Excel rows if this file is already loaded
        with get_connection() as conn, conn.cursor() as cur:
            existing = find_batch_by_hash(cur, source.sha256)
        if existing:
            logger.info(kv(step="load_bronze", event="skip_known_file", batch_id=existing[0]))
            return BronzeLoadResult(existing[0], existing[1], skipped=True).to_dict()

        frame = read_source(source.path)
        with get_connection() as conn, conn.cursor() as cur:
            result = load_bronze(cur, source, frame)
        return {**result.to_dict(), "rows_read": len(frame), "rows_written": result.rows_loaded}

    @task
    def transform_to_staging(bronze_info: dict) -> dict:
        """Clean the Bronze batch and stage it. Nothing reaches Silver in this task."""
        batch_id = bronze_info["batch_id"]
        with get_connection() as conn, conn.cursor() as cur:
            raw = read_bronze_batch(cur, batch_id)
            result = clean_sales(raw, batch_id)
            staged = write_staging(cur, result.clean)
            rejected = write_rejected(cur, result.rejected, batch_id)
        return {
            "batch_id": batch_id,
            "rows_read": len(raw),
            "rows_written": staged + rejected,
            "rows_staged": staged,
            "rows_rejected": rejected,
            "rejection_counts": result.rejection_counts,
        }

    def _quality_gate(checks: list, batch_id: str) -> dict:
        run_id = get_current_context()["run_id"]
        # Results are committed first, so failed checks stay visible in audit.quality_checks
        with get_connection() as conn, conn.cursor() as cur:
            results = run_checks(cur, checks, batch_id)
            record_results(cur, results, dag_run_id=run_id, batch_id=batch_id)
        try:
            raise_on_critical_failures(results)
        except DataQualityError as exc:
            raise AirflowFailException(str(exc)) from exc  # no retry: the data won't change
        return {
            "batch_id": batch_id,
            "checks_passed": sum(r.passed for r in results),
            "warnings": [r.check.name for r in results if not r.passed],
        }

    @task(retries=0)
    def validate_staging(staging_info: dict) -> dict:
        """Quality gate before Silver."""
        return _quality_gate(STAGING_CHECKS, staging_info["batch_id"])

    @task
    def load_silver(staging_info: dict) -> dict:
        """Upsert staging into Silver in a single transaction."""
        with get_connection() as conn, conn.cursor() as cur:
            upserted = merge_staging_into_silver(cur)
        return {"batch_id": staging_info["batch_id"], "rows_written": upserted}

    bronze = load_to_bronze(extract_source())

    @task
    def build_gold_layer(silver_info: dict) -> dict:
        """Upsert dimensions, then the fact, in one transaction."""
        with get_connection() as conn, conn.cursor() as cur:
            changed = build_gold(cur)
        return {
            "batch_id": silver_info["batch_id"],
            "rows_written": sum(changed.values()),
            "rows_changed": changed,
        }

    @task(retries=0)
    def validate_gold(gold_info: dict) -> dict:
        """Reconciliation: Gold must match Silver line by line and in total revenue."""
        return _quality_gate(GOLD_CHECKS, gold_info["batch_id"])

    silver = load_silver(validate_staging(transform_to_staging(bronze)))
    validate_gold(build_gold_layer(silver))


sales_pipeline()
