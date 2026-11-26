"""Write one row per task execution into audit.pipeline_runs.

Called from Airflow callbacks, on its own connection, so a failure is recorded
even when the task's own transaction was rolled back. Recording never raises:
an audit problem must not hide or replace the real task outcome.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from dataflow.database.connection import get_connection
from dataflow.logging_config import kv

logger = logging.getLogger(__name__)

MAX_ERROR_LENGTH = 2000


@dataclass(frozen=True)
class TaskRun:
    dag_run_id: str
    task_id: str
    status: str  # success | failed | skipped
    batch_id: str | None = None
    rows_read: int | None = None
    rows_written: int | None = None
    error_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


def task_run_from_result(
    dag_run_id: str, task_id: str, result: dict | None, started_at: datetime | None
) -> TaskRun:
    """Build a success record from the dict a task returned (its XCom)."""
    result = result or {}
    return TaskRun(
        dag_run_id=dag_run_id,
        task_id=task_id,
        status="skipped" if result.get("skipped") else "success",
        batch_id=result.get("batch_id"),
        rows_read=result.get("rows_read"),
        rows_written=result.get("rows_written"),
        started_at=started_at,
        finished_at=datetime.now(UTC),
    )


def record_task_run(run: TaskRun) -> bool:
    """Insert the record; returns False (and logs) instead of raising on error."""
    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.pipeline_runs
                    (dag_run_id, task_id, batch_id, status, rows_read, rows_written,
                     error_message, started_at, finished_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, COALESCE(%s, now()), %s)
                """,
                (
                    run.dag_run_id,
                    run.task_id,
                    run.batch_id,
                    run.status,
                    run.rows_read,
                    run.rows_written,
                    (run.error_message or "")[:MAX_ERROR_LENGTH] or None,
                    run.started_at,
                    run.finished_at or datetime.now(UTC),
                ),
            )
    except Exception:  # noqa: BLE001 - auditing must never break the pipeline
        logger.exception(kv(step="audit", event="record_failed", task=run.task_id))
        return False
    logger.info(kv(step="audit", task=run.task_id, status=run.status, batch_id=run.batch_id))
    return True
