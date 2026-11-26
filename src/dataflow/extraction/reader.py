"""Read the Online Retail II workbook into a single raw DataFrame.

Every value is read as text: Bronze stores the data as received, and typing
is the Silver layer's job.
"""

from __future__ import annotations

import io
import logging
import zipfile
from pathlib import Path

import pandas as pd

from dataflow.logging_config import kv

logger = logging.getLogger(__name__)

# Source column -> Bronze column
SOURCE_COLUMNS: dict[str, str] = {
    "Invoice": "invoice",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "Price": "price",
    "Customer ID": "customer_id",
    "Country": "country",
}

BRONZE_COLUMNS: list[str] = ["source_sheet", "source_row", *SOURCE_COLUMNS.values()]

# Header is spreadsheet row 1, so the first data row is row 2
_FIRST_DATA_ROW = 2


class SourceSchemaError(ValueError):
    """Raised when the source file does not have the expected columns."""


def _open_workbook(path: Path) -> io.BytesIO:
    """Return the .xlsx bytes, whether `path` is the workbook or a ZIP containing it."""
    if path.suffix.lower() == ".xlsx":
        return io.BytesIO(path.read_bytes())
    with zipfile.ZipFile(path) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".xlsx")]
        if len(members) != 1:
            raise SourceSchemaError(
                f"Expected exactly one .xlsx inside {path.name}, found {members}"
            )
        return io.BytesIO(archive.read(members[0]))


def read_source(path: Path) -> pd.DataFrame:
    """Read every sheet, check the columns and return rows ready for Bronze."""
    sheets = pd.read_excel(_open_workbook(path), sheet_name=None, dtype=str, engine="openpyxl")

    frames = []
    for sheet_name, frame in sheets.items():
        missing = set(SOURCE_COLUMNS) - set(frame.columns)
        if missing:
            raise SourceSchemaError(f"Sheet {sheet_name!r} is missing columns: {sorted(missing)}")
        frame = frame[list(SOURCE_COLUMNS)].rename(columns=SOURCE_COLUMNS)
        frame.insert(0, "source_row", range(_FIRST_DATA_ROW, _FIRST_DATA_ROW + len(frame)))
        frame.insert(0, "source_sheet", sheet_name)
        frames.append(frame)
        logger.info(kv(step="read_source", sheet=sheet_name, rows=len(frame)))

    if not frames:
        raise SourceSchemaError(f"No sheets found in {path.name}")
    return pd.concat(frames, ignore_index=True)[BRONZE_COLUMNS]
