from datetime import UTC, datetime

import pandas as pd
import pytest

from dataflow.transformation.cleaning import (
    STAGING_COLUMNS,
    RejectionReason,
    clean_sales,
)

BATCH = "00000000-0000-0000-0000-000000000001"
NOW = datetime(2026, 11, 25, tzinfo=UTC)

VALID = {
    "invoice": "489434",
    "stock_code": "85048",
    "description": "15CM CHRISTMAS GLASS BALL 20 LIGHTS",
    "quantity": "12",
    "invoice_date": "2009-12-01 07:45:00",
    "price": "6.95",
    "customer_id": "13085.0",
    "country": "United Kingdom",
}


def raw(*rows: dict) -> pd.DataFrame:
    """Build Bronze-like rows (all text), each overriding the VALID defaults."""
    records = [{**VALID, **row, "raw_id": str(i + 1)} for i, row in enumerate(rows)]
    return pd.DataFrame(records, dtype="string")


def reasons(result) -> dict[int, str]:
    return dict(zip(result.rejected["raw_id"], result.rejected["rejection_reason"], strict=True))


def test_valid_row_is_typed() -> None:
    result = clean_sales(raw({}), BATCH, NOW)

    row = result.clean.iloc[0]
    assert list(result.clean.columns) == STAGING_COLUMNS
    assert row["quantity"] == 12
    assert row["unit_price"] == pytest.approx(6.95)
    assert row["invoice_ts"] == pd.Timestamp("2009-12-01 07:45:00")
    assert row["customer_id"] == 13085
    assert not row["is_cancellation"]
    assert len(row["line_key"]) == 32
    assert result.rejected.empty


def test_text_is_trimmed_and_standardised() -> None:
    result = clean_sales(
        raw(
            {"description": "  pink   cherry lights ", "stock_code": " 79323p ", "country": "EIRE"}
        ),
        BATCH,
        NOW,
    )

    row = result.clean.iloc[0]
    assert row["description"] == "PINK CHERRY LIGHTS"
    assert row["stock_code"] == "79323P"
    assert row["country"] == "Ireland"


def test_cancellation_is_kept_and_flagged() -> None:
    result = clean_sales(raw({"invoice": "C489449", "quantity": "-12"}), BATCH, NOW)

    row = result.clean.iloc[0]
    assert row["is_cancellation"]
    assert row["quantity"] == -12


def test_missing_customer_is_kept() -> None:
    result = clean_sales(raw({"customer_id": None}), BATCH, NOW)

    assert len(result.clean) == 1
    assert pd.isna(result.clean.iloc[0]["customer_id"])


def test_missing_country_becomes_unknown() -> None:
    result = clean_sales(
        raw({"country": None}, {"country": "Unspecified", "invoice": "489435"}), BATCH, NOW
    )

    assert result.clean["country"].tolist() == ["Unknown", "Unknown"]


@pytest.mark.parametrize(
    ("override", "expected"),
    [
        ({"invoice": None}, RejectionReason.MISSING_REQUIRED),
        ({"stock_code": "  "}, RejectionReason.MISSING_REQUIRED),
        ({"quantity": "abc"}, RejectionReason.INVALID_QUANTITY),
        ({"quantity": "1.5"}, RejectionReason.INVALID_QUANTITY),
        ({"quantity": "0"}, RejectionReason.ZERO_QUANTITY),
        ({"price": "free"}, RejectionReason.INVALID_PRICE),
        ({"price": "0"}, RejectionReason.NON_POSITIVE_PRICE),
        ({"price": "-11062.06"}, RejectionReason.NON_POSITIVE_PRICE),
        ({"invoice_date": "not a date"}, RejectionReason.INVALID_DATE),
        ({"invoice_date": "2030-01-01 00:00:00"}, RejectionReason.INVALID_DATE),
        ({"stock_code": "POST"}, RejectionReason.NON_PRODUCT_CODE),
        ({"stock_code": "bank charges"}, RejectionReason.NON_PRODUCT_CODE),
        ({"stock_code": "gift_0001_20"}, RejectionReason.NON_PRODUCT_CODE),
    ],
)
def test_invalid_rows_are_quarantined_with_reason(override: dict, expected: str) -> None:
    result = clean_sales(raw(override), BATCH, NOW)

    assert result.clean.empty
    assert reasons(result) == {1: expected}


def test_first_failing_rule_wins() -> None:
    result = clean_sales(raw({"quantity": "0", "price": "0"}), BATCH, NOW)

    assert reasons(result) == {1: RejectionReason.ZERO_QUANTITY}


def test_exact_duplicates_keep_first_occurrence() -> None:
    result = clean_sales(raw({}, {}, {"quantity": "6"}), BATCH, NOW)

    assert len(result.clean) == 2
    assert reasons(result) == {2: RejectionReason.DUPLICATE}


def test_line_key_is_deterministic() -> None:
    first = clean_sales(raw({}), BATCH, NOW).clean.iloc[0]["line_key"]
    second = clean_sales(raw({}), "another-batch", NOW).clean.iloc[0]["line_key"]

    assert first == second


def test_every_input_row_is_either_clean_or_rejected() -> None:
    data = raw({}, {}, {"price": "0"}, {"invoice": "C1", "quantity": "-1"}, {"stock_code": "M"})

    result = clean_sales(data, BATCH, NOW)

    assert len(result.clean) + len(result.rejected) == len(data)
    assert result.rejection_counts == {
        RejectionReason.DUPLICATE: 1,
        RejectionReason.NON_POSITIVE_PRICE: 1,
        RejectionReason.NON_PRODUCT_CODE: 1,
    }
