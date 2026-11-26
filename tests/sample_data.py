"""Builds a tiny Online Retail II workbook for tests, with the real column names
and the kinds of problems found in the real file."""

from __future__ import annotations

import io
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd

SHEET_2009 = pd.DataFrame(
    {
        "Invoice": ["489434", "489434", "C489449"],
        "StockCode": ["85048", "79323P", "22087"],
        "Description": ["15CM CHRISTMAS GLASS BALL 20 LIGHTS", "PINK CHERRY LIGHTS", None],
        "Quantity": [12, 12, -12],
        "InvoiceDate": [
            datetime(2009, 12, 1, 7, 45),
            datetime(2009, 12, 1, 7, 45),
            datetime(2009, 12, 1, 10, 33),
        ],
        "Price": [6.95, 6.75, 2.95],
        "Customer ID": [13085.0, 13085.0, None],
        "Country": ["United Kingdom", "United Kingdom", "EIRE"],
    }
)

SHEET_2010 = pd.DataFrame(
    {
        "Invoice": ["536365"],
        "StockCode": ["POST"],
        "Description": ["POSTAGE"],
        "Quantity": [1],
        "InvoiceDate": [datetime(2010, 12, 1, 8, 26)],
        "Price": [18.0],
        "Customer ID": [17850.0],
        "Country": ["France"],
    }
)

TOTAL_ROWS = len(SHEET_2009) + len(SHEET_2010)


def write_workbook(path: Path, sheets: dict[str, pd.DataFrame] | None = None) -> Path:
    sheets = sheets or {"Year 2009-2010": SHEET_2009, "Year 2010-2011": SHEET_2010}
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)
    return path


def write_zip(path: Path, sheets: dict[str, pd.DataFrame] | None = None) -> Path:
    buffer = io.BytesIO()
    write_workbook(buffer, sheets)  # type: ignore[arg-type]
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("online_retail_II.xlsx", buffer.getvalue())
    return path


def healthy_sheets(valid_rows: int = 30) -> dict[str, pd.DataFrame]:
    """The sample sheets plus many valid lines, so batch-level rates are realistic
    (rejections well under 10%, most lines with a customer)."""
    extra = pd.DataFrame(
        {
            "Invoice": [str(490000 + i) for i in range(valid_rows)],
            "StockCode": ["21232"] * valid_rows,
            "Description": ["STRAWBERRY CERAMIC TRINKET BOX"] * valid_rows,
            "Quantity": [6] * valid_rows,
            "InvoiceDate": [datetime(2010, 3, 1, 12, 0)] * valid_rows,
            "Price": [1.25] * valid_rows,
            "Customer ID": [12346.0 + i for i in range(valid_rows)],
            "Country": ["United Kingdom"] * valid_rows,
        }
    )
    return {"Year 2009-2010": pd.concat([SHEET_2009, extra]), "Year 2010-2011": SHEET_2010}
