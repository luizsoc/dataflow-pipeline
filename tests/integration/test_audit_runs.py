"""record_task_run commits on its own connection (that is the point: a failure
must be recorded even if the task's transaction rolled back), so this test
cleans up after itself."""

import uuid

import pytest

from dataflow.database.audit import TaskRun, record_task_run

pytestmark = pytest.mark.integration


def test_failure_is_recorded_with_error_message(dw_conn) -> None:
    run_id = f"test-{uuid.uuid4()}"
    try:
        ok = record_task_run(
            TaskRun(run_id, "extract_source", "failed", error_message="HTTPError: 404")
        )
        with dw_conn.cursor() as cur:
            cur.execute(
                "SELECT task_id, status, error_message, finished_at IS NOT NULL "
                "FROM audit.pipeline_runs WHERE dag_run_id = %s",
                (run_id,),
            )
            rows = cur.fetchall()
    finally:
        with dw_conn.cursor() as cur:
            cur.execute("DELETE FROM audit.pipeline_runs WHERE dag_run_id = %s", (run_id,))
        dw_conn.commit()

    assert ok is True
    assert rows == [("extract_source", "failed", "HTTPError: 404", True)]
