from pathlib import Path

import pytest

from dataflow.config import DEFAULT_SOURCE_URL, ConfigError, PipelineSettings, WarehouseSettings


def test_warehouse_settings_read_from_env(warehouse_env: None) -> None:
    settings = WarehouseSettings.from_env()

    assert settings.host == "db.local"
    assert settings.port == 5432
    assert settings.dbname == "dataflow"


@pytest.mark.parametrize("missing", ["DW_DB", "DW_USER", "DW_PASSWORD"])
def test_missing_required_variable_fails_fast(
    warehouse_env: None, monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    monkeypatch.delenv(missing)

    with pytest.raises(ConfigError, match=missing):
        WarehouseSettings.from_env()


def test_invalid_port_is_rejected(warehouse_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DW_PORT", "not-a-port")

    with pytest.raises(ConfigError, match="DW_PORT"):
        WarehouseSettings.from_env()


def test_password_never_appears_in_repr(warehouse_env: None) -> None:
    text = repr(WarehouseSettings.from_env())

    assert "s3cret" not in text
    assert "***" in text


def test_pipeline_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("SOURCE_URL", "DATA_DIR", "LOG_LEVEL"):
        monkeypatch.delenv(name, raising=False)

    settings = PipelineSettings.from_env()

    assert settings.source_url == DEFAULT_SOURCE_URL
    assert settings.raw_dir == Path("data") / "raw"
    assert settings.log_level == "INFO"
