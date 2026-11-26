from datetime import UTC, datetime

import pytest

from dataflow.database.audit import TaskRun, record_task_run, task_run_from_result

STARTED = datetime(2026, 11, 26, 10, 0, tzinfo=UTC)


def test_success_record_takes_counts_from_task_result() -> None:
    run = task_run_from_result(
        "run-1",
        "load_silver",
        {"batch_id": "b1", "rows_read": 10, "rows_written": 8},
        started_at=STARTED,
    )

    assert (run.status, run.batch_id, run.rows_read, run.rows_written) == ("success", "b1", 10, 8)
    assert run.started_at == STARTED
    assert run.finished_at is not None


def test_skipped_bronze_load_is_recorded_as_skipped() -> None:
    run = task_run_from_result("run-1", "load_to_bronze", {"skipped": True}, started_at=None)

    assert run.status == "skipped"


def test_task_without_return_value_is_still_recorded() -> None:
    run = task_run_from_result("run-1", "validate_gold", None, started_at=None)

    assert run.status == "success"
    assert run.rows_written is None


def test_audit_failure_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DW_PASSWORD", raising=False)  # connection cannot be configured

    assert record_task_run(TaskRun("run-1", "extract_source", "failed")) is False
