from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow_home_"))
airflow = pytest.importorskip("airflow")
from airflow.models import DagBag


def test_airflow_dag_parses_without_import_errors(tmp_path):
    dags_path = Path(__file__).resolve().parents[1] / "dags"
    dag_bag = DagBag(dag_folder=str(dags_path), include_examples=False)

    assert dag_bag.import_errors == {}
    assert "nba_analytics_pipeline" in dag_bag.dags
