from datetime import date

import pytest
import requests

from dataflow.extraction import download as download_module
from dataflow.extraction.download import (
    DownloadedFile,
    ExtractionError,
    download_source,
    file_name_from_url,
)

URL = "https://example.org/static/public/502/online+retail+ii.zip"
DAY = date(2026, 11, 25)


class FakeResponse:
    def __init__(self, content: bytes, status: int = 200) -> None:
        self._content = content
        self.status_code = status

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def iter_content(self, chunk_size: int):
        yield self._content


def test_file_name_from_url() -> None:
    assert file_name_from_url(URL) == "online_retail_ii.zip"


def test_download_writes_into_date_partition(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(download_module.requests, "get", lambda *a, **k: FakeResponse(b"abc"))

    result = download_source(URL, tmp_path, DAY)

    assert result.path == tmp_path / "ingestion_date=2026-11-25" / "online_retail_ii.zip"
    assert result.path.read_bytes() == b"abc"
    # sha256 of b"abc"
    assert result.sha256 == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    assert result.size_bytes == 3


def test_existing_file_is_reused_without_downloading(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(download_module.requests, "get", lambda *a, **k: FakeResponse(b"abc"))
    first = download_source(URL, tmp_path, DAY)

    def fail(*args, **kwargs):
        raise AssertionError("must not download again")

    monkeypatch.setattr(download_module.requests, "get", fail)
    second = download_source(URL, tmp_path, DAY)

    assert second.sha256 == first.sha256


def test_http_error_leaves_no_partial_file(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(download_module.requests, "get", lambda *a, **k: FakeResponse(b"", 503))

    with pytest.raises(ExtractionError, match="503"):
        download_source(URL, tmp_path, DAY)

    assert list((tmp_path / "ingestion_date=2026-11-25").iterdir()) == []


@pytest.mark.parametrize(("status", "retryable"), [(404, False), (403, False), (503, True)])
def test_only_server_errors_are_retryable(tmp_path, monkeypatch, status, retryable) -> None:
    monkeypatch.setattr(download_module.requests, "get", lambda *a, **k: FakeResponse(b"", status))

    with pytest.raises(ExtractionError) as exc_info:
        download_source(URL, tmp_path, DAY)

    assert exc_info.value.retryable is retryable


def test_timeout_is_retryable(tmp_path, monkeypatch) -> None:
    def timeout(*args, **kwargs):
        raise requests.Timeout("read timed out")

    monkeypatch.setattr(download_module.requests, "get", timeout)

    with pytest.raises(ExtractionError) as exc_info:
        download_source(URL, tmp_path, DAY)

    assert exc_info.value.retryable is True


def test_empty_file_is_rejected(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(download_module.requests, "get", lambda *a, **k: FakeResponse(b""))

    with pytest.raises(ExtractionError, match="empty") as exc_info:
        download_source(URL, tmp_path, DAY)

    assert exc_info.value.retryable is False


def test_round_trip_through_xcom_dict(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(download_module.requests, "get", lambda *a, **k: FakeResponse(b"abc"))
    original = download_source(URL, tmp_path, DAY)

    assert DownloadedFile.from_dict(original.to_dict()) == original
