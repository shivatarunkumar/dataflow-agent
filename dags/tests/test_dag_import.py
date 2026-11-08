"""DAG integrity: both DAGs import and have the expected shape. Skipped when Airflow isn't installed locally."""

import pytest

pytest.importorskip("airflow")
pytest.importorskip("airflow.providers.google")


@pytest.fixture(scope="module")
def dagbag():
    from pathlib import Path

    from airflow.models import DagBag

    return DagBag(dag_folder=str(Path(__file__).resolve().parents[1]), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_ingest_dag_shape(dagbag):
    dag = dagbag.get_dag("gcs_to_bq_dataflow")
    assert {t.task_id for t in dag.tasks} == {"register_job", "is_new_file", "run_dataflow", "finalize"}
    assert dag.get_task("finalize").trigger_rule == "all_done"
    assert dag.get_task("run_dataflow").max_active_tis_per_dag >= 1


def test_listener_dag_shape(dagbag):
    dag = dagbag.get_dag("pubsub_file_listener")
    assert {t.task_id for t in dag.tasks} == {"pull_file_notifications", "trigger_ingestion"}
