import pytest

from dataflow.validation import (
    STAGING_CHECKS,
    CheckResult,
    DataQualityError,
    QualityCheck,
    Severity,
    raise_on_critical_failures,
)


def make_check(severity: Severity) -> QualityCheck:
    return QualityCheck(
        name=f"{severity.value}_check",
        layer="staging",
        severity=severity,
        description="test",
        sql="SELECT 0",
    )


def test_check_names_are_unique() -> None:
    names = [check.name for check in STAGING_CHECKS]
    assert len(names) == len(set(names))


def test_result_passes_only_with_zero_failed_rows() -> None:
    check = make_check(Severity.CRITICAL)
    assert CheckResult(check, 0).passed
    assert not CheckResult(check, 3).passed


def test_failed_warning_does_not_stop_the_pipeline() -> None:
    raise_on_critical_failures([CheckResult(make_check(Severity.WARNING), 10)])


def test_failed_critical_stops_the_pipeline() -> None:
    results = [
        CheckResult(make_check(Severity.CRITICAL), 5),
        CheckResult(make_check(Severity.WARNING), 10),
    ]

    with pytest.raises(DataQualityError, match=r"critical_check \(5 rows\)") as exc_info:
        raise_on_critical_failures(results)

    assert [r.check.name for r in exc_info.value.failures] == ["critical_check"]
