"""Download the source file and fingerprint it.

The file is written to a temporary name and renamed only when complete, so a
failed download never leaves a half-written file that looks valid.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests

from dataflow.logging_config import kv

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024  # 1 MiB


class ExtractionError(RuntimeError):
    """Raised when the source cannot be downloaded or is not usable.

    `retryable` tells the orchestrator whether trying again can help: a timeout
    or a 5xx may be transient, a 404 or an empty file will not fix itself.
    """

    def __init__(self, message: str, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = retryable


@dataclass(frozen=True)
class DownloadedFile:
    path: Path
    sha256: str
    size_bytes: int
    source_url: str
    extracted_at: datetime

    def to_dict(self) -> dict[str, str | int]:
        """JSON-friendly form, small enough to pass between Airflow tasks via XCom."""
        data = asdict(self)
        data["path"] = str(self.path)
        data["extracted_at"] = self.extracted_at.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: dict) -> DownloadedFile:
        return cls(
            path=Path(data["path"]),
            sha256=data["sha256"],
            size_bytes=int(data["size_bytes"]),
            source_url=data["source_url"],
            extracted_at=datetime.fromisoformat(data["extracted_at"]),
        )


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_name_from_url(url: str) -> str:
    name = Path(unquote(urlparse(url).path)).name
    if not name:
        raise ExtractionError(f"Cannot derive a file name from URL: {url}", retryable=False)
    return name.replace("+", "_")


def partition_dir(raw_dir: Path, ingestion_date: date) -> Path:
    """Hive-style partition: data/raw/ingestion_date=YYYY-MM-DD/."""
    return raw_dir / f"ingestion_date={ingestion_date.isoformat()}"


def download_source(
    url: str,
    raw_dir: Path,
    ingestion_date: date,
    timeout_seconds: int = 60,
) -> DownloadedFile:
    """Download `url` into the partition for `ingestion_date`.

    If the file already exists in that partition (e.g. a retried task), it is
    reused instead of downloaded again.
    """
    target_dir = partition_dir(raw_dir, ingestion_date)
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / file_name_from_url(url)

    if target.exists():
        logger.info(kv(step="extract", event="reuse_existing_file", path=target))
    else:
        tmp = target.with_suffix(target.suffix + ".part")
        logger.info(kv(step="extract", event="download_started", url=url))
        try:
            with requests.get(url, stream=True, timeout=timeout_seconds) as response:
                response.raise_for_status()
                with tmp.open("wb") as fh:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        fh.write(chunk)
        except requests.HTTPError as exc:
            tmp.unlink(missing_ok=True)
            status = exc.response.status_code if exc.response is not None else None
            # 4xx = wrong URL or no permission: retrying will not help
            client_error = status is not None and 400 <= status < 500
            raise ExtractionError(
                f"Download failed for {url}: {exc}", retryable=not client_error
            ) from exc
        except requests.RequestException as exc:  # timeouts, DNS, connection resets
            tmp.unlink(missing_ok=True)
            raise ExtractionError(f"Download failed for {url}: {exc}") from exc
        tmp.replace(target)

    size = target.stat().st_size
    if size == 0:
        target.unlink()
        raise ExtractionError(f"Downloaded file is empty: {url}", retryable=False)

    result = DownloadedFile(
        path=target,
        sha256=file_sha256(target),
        size_bytes=size,
        source_url=url,
        extracted_at=datetime.now(UTC),
    )
    logger.info(
        kv(step="extract", event="file_ready", path=target, bytes=size, sha256=result.sha256)
    )
    return result
