"""Shared pytest fixtures."""

from __future__ import annotations

import pytest

from dataflow.config import ConfigError, WarehouseSettings


@pytest.fixture
def warehouse_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """A complete, fake warehouse configuration for unit tests."""
    monkeypatch.setenv("DW_HOST", "db.local")
    monkeypatch.setenv("DW_PORT", "5432")
    monkeypatch.setenv("DW_DB", "dataflow")
    monkeypatch.setenv("DW_USER", "dataflow")
    monkeypatch.setenv("DW_PASSWORD", "s3cret")


@pytest.fixture
def dw_conn():
    """A live warehouse connection; skips the test when the database is unreachable."""
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        settings = WarehouseSettings.from_env()
        conn = psycopg2.connect(
            host=settings.host,
            port=settings.port,
            dbname=settings.dbname,
            user=settings.user,
            password=settings.password,
            connect_timeout=3,
        )
    except (ConfigError, psycopg2.OperationalError) as exc:
        pytest.skip(f"warehouse not reachable: {exc}")
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()
