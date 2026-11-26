import pytest

from dataflow.extraction.reader import BRONZE_COLUMNS, SourceSchemaError, read_source
from tests.sample_data import SHEET_2009, TOTAL_ROWS, write_workbook, write_zip


def test_reads_all_sheets_from_zip(tmp_path) -> None:
    frame = read_source(write_zip(tmp_path / "online_retail_II.zip"))

    assert list(frame.columns) == BRONZE_COLUMNS
    assert len(frame) == TOTAL_ROWS
    assert set(frame["source_sheet"]) == {"Year 2009-2010", "Year 2010-2011"}


def test_source_row_matches_spreadsheet_row(tmp_path) -> None:
    frame = read_source(write_workbook(tmp_path / "data.xlsx"))

    first_sheet = frame[frame["source_sheet"] == "Year 2009-2010"]
    assert first_sheet["source_row"].tolist() == [2, 3, 4]  # row 1 is the header


def test_values_are_kept_as_text(tmp_path) -> None:
    frame = read_source(write_workbook(tmp_path / "data.xlsx"))
    row = frame.iloc[0]

    assert row["invoice"] == "489434"
    assert row["quantity"] == "12"
    assert row["invoice_date"].startswith("2009-12-01 07:45")
    assert row["customer_id"].startswith("13085")


def test_missing_values_stay_missing(tmp_path) -> None:
    frame = read_source(write_workbook(tmp_path / "data.xlsx"))
    cancelled = frame[frame["invoice"] == "C489449"].iloc[0]

    assert cancelled["description"] is None or cancelled.isna()["description"]
    assert cancelled.isna()["customer_id"]


def test_missing_column_is_rejected(tmp_path) -> None:
    broken = SHEET_2009.drop(columns=["Price"])
    path = write_workbook(tmp_path / "broken.xlsx", {"Sheet1": broken})

    with pytest.raises(SourceSchemaError, match="Price"):
        read_source(path)
