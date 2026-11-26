from dataflow.logging_config import kv


def test_kv_renders_key_value_pairs() -> None:
    assert kv(step="load_bronze", rows=10) == "step=load_bronze rows=10"


def test_kv_quotes_values_with_spaces() -> None:
    assert kv(error="file not found") == 'error="file not found"'
