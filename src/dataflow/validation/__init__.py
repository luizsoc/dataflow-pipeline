"""Validation layer: automated data quality checks."""

from dataflow.validation.checks import (
    GOLD_CHECKS,
    STAGING_CHECKS,
    CheckResult,
    DataQualityError,
    QualityCheck,
    Severity,
    raise_on_critical_failures,
    record_results,
    run_checks,
)

__all__ = [
    "GOLD_CHECKS",
    "STAGING_CHECKS",
    "CheckResult",
    "DataQualityError",
    "QualityCheck",
    "Severity",
    "raise_on_critical_failures",
    "record_results",
    "run_checks",
]
