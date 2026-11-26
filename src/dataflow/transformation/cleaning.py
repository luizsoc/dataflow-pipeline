"""Bronze -> Silver cleaning rules, as pure functions on DataFrames.

Input: raw Bronze rows (all text). Output: typed rows for silver.sales_staging,
plus the rejected rows with the reason, for silver.sales_rejected.

Each row gets the FIRST rule it fails, so every rejection has exactly one reason.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

import pandas as pd

from dataflow.logging_config import kv

logger = logging.getLogger(__name__)

# Stock codes that are not products: postage, fees, manual adjustments, tests.
NON_PRODUCT_CODES: frozenset[str] = frozenset(
    {
        "POST",
        "DOT",
        "C2",
        "M",
        "D",
        "S",
        "B",
        "CRUK",
        "PADS",
        "BANK CHARGES",
        "AMAZONFEE",
        "ADJUST",
        "ADJUST2",
    }
)
NON_PRODUCT_PREFIXES: tuple[str, ...] = ("GIFT_", "TEST")

COUNTRY_FIXES: dict[str, str] = {
    "EIRE": "Ireland",
    "RSA": "South Africa",
    "USA": "United States",
    "Unspecified": "Unknown",
}
UNKNOWN_COUNTRY = "Unknown"

STAGING_COLUMNS: list[str] = [
    "line_key",
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "unit_price",
    "invoice_ts",
    "customer_id",
    "country",
    "is_cancellation",
    "batch_id",
]

# Columns that identify a sales line; identical values = duplicate line
_KEY_COLUMNS = ["invoice_no", "stock_code", "quantity", "unit_price", "invoice_ts", "customer_id"]


class RejectionReason:
    MISSING_REQUIRED = "missing_required_field"
    INVALID_QUANTITY = "invalid_quantity"
    ZERO_QUANTITY = "zero_quantity"
    INVALID_PRICE = "invalid_price"
    NON_POSITIVE_PRICE = "non_positive_price"
    INVALID_DATE = "invalid_date"
    NON_PRODUCT_CODE = "non_product_code"
    DUPLICATE = "duplicate_line"


@dataclass(frozen=True)
class CleaningResult:
    clean: pd.DataFrame  # STAGING_COLUMNS
    rejected: pd.DataFrame  # raw_id, rejection_reason

    @property
    def rejection_counts(self) -> dict[str, int]:
        counts = self.rejected["rejection_reason"].value_counts()
        return {str(reason): int(count) for reason, count in counts.items()}


def _strip_text(series: pd.Series) -> pd.Series:
    """Trim, collapse inner whitespace and turn empty strings into missing values."""
    cleaned = series.astype("string").str.strip().str.replace(r"\s+", " ", regex=True)
    return cleaned.mask(cleaned == "")


def _line_key(frame: pd.DataFrame) -> pd.Series:
    """Deterministic MD5 of the key columns: same line, same key, on every run."""
    if frame.empty:
        return pd.Series(index=frame.index, dtype="string")
    parts = [frame[col].astype("string").fillna("") for col in _KEY_COLUMNS]
    joined = parts[0].str.cat(parts[1:], sep="|")  # vectorised, far faster than row-wise apply
    return pd.Series(
        [hashlib.md5(text.encode("utf-8")).hexdigest() for text in joined],
        index=frame.index,
        dtype="string",
    )


def is_non_product(stock_code: pd.Series) -> pd.Series:
    upper = stock_code.str.upper()
    return upper.isin(NON_PRODUCT_CODES) | upper.str.startswith(NON_PRODUCT_PREFIXES)


def clean_sales(raw: pd.DataFrame, batch_id: str, now: datetime | None = None) -> CleaningResult:
    """Apply every Silver rule to a Bronze batch."""
    now = now or datetime.now(UTC)
    df = pd.DataFrame({"raw_id": raw["raw_id"].astype("int64")})
    reason = pd.Series(pd.NA, index=raw.index, dtype="string")

    def reject(mask: pd.Series, why: str) -> None:
        reason[mask.fillna(False).astype(bool) & reason.isna()] = why

    # Text columns
    df["invoice_no"] = _strip_text(raw["invoice"]).str.upper()
    df["stock_code"] = _strip_text(raw["stock_code"]).str.upper()
    df["description"] = _strip_text(raw["description"]).str.upper()
    country = _strip_text(raw["country"]).replace(COUNTRY_FIXES)
    df["country"] = country.fillna(UNKNOWN_COUNTRY)

    reject(df["invoice_no"].isna() | df["stock_code"].isna(), RejectionReason.MISSING_REQUIRED)

    # Quantity: must be a non-zero integer
    quantity = pd.to_numeric(_strip_text(raw["quantity"]), errors="coerce")
    reject(quantity.isna() | (quantity % 1 != 0), RejectionReason.INVALID_QUANTITY)
    reject(quantity == 0, RejectionReason.ZERO_QUANTITY)
    df["quantity"] = quantity.where(reason.isna()).astype("Int64")

    # Price: numeric and positive (zero-price lines are adjustments, not sales)
    price = pd.to_numeric(_strip_text(raw["price"]), errors="coerce")
    reject(price.isna(), RejectionReason.INVALID_PRICE)
    reject(price <= 0, RejectionReason.NON_POSITIVE_PRICE)
    df["unit_price"] = price.round(2)

    # Date: parseable and not in the future
    invoice_ts = pd.to_datetime(_strip_text(raw["invoice_date"]), errors="coerce", format="ISO8601")
    reject(
        invoice_ts.isna() | (invoice_ts > now.replace(tzinfo=None)), RejectionReason.INVALID_DATE
    )
    df["invoice_ts"] = invoice_ts

    reject(is_non_product(df["stock_code"].fillna("")), RejectionReason.NON_PRODUCT_CODE)

    # Customer is optional: sales without a customer still count (unknown member in Gold)
    customer = pd.to_numeric(_strip_text(raw["customer_id"]), errors="coerce")
    df["customer_id"] = customer.where(customer % 1 == 0).astype("Int64")

    df["is_cancellation"] = df["invoice_no"].str.startswith("C").fillna(False).astype(bool)
    df["batch_id"] = batch_id

    # Duplicates: among valid rows, keep the first occurrence of each line_key
    valid = df[reason.isna()].copy()
    valid["line_key"] = _line_key(valid)
    duplicated = valid["line_key"].duplicated(keep="first")
    reason.loc[valid.index[duplicated]] = RejectionReason.DUPLICATE
    clean = valid[~duplicated]

    rejected = pd.DataFrame({"raw_id": df["raw_id"], "rejection_reason": reason})
    rejected = rejected[reason.notna()].reset_index(drop=True)

    result = CleaningResult(clean=clean[STAGING_COLUMNS].reset_index(drop=True), rejected=rejected)
    logger.info(
        kv(
            step="clean",
            batch_id=batch_id,
            rows_in=len(raw),
            rows_clean=len(result.clean),
            rows_rejected=len(rejected),
            reasons=result.rejection_counts,
        )
    )
    return result
