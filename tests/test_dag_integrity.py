"""Every DAG file must import cleanly. Runs inside the Airflow container."""

from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


def test_dags_import_without_errors() -> None:
    dagbag_module = pytest.importorskip(
        "airflow.dag_processing.dagbag", reason="Airflow is only installed in the image"
    )

    # Airflow 3: DagBag lives in airflow.dag_processing and never loads example DAGs
    dag_bag = dagbag_module.DagBag(dag_folder=str(DAGS_DIR))

    assert dag_bag.import_errors == {}
