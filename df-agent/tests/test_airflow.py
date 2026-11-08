"""Airflow (schema v2) events: top-down investigation Airflow -> Dataflow -> BigQuery (no LLM, no GCP)."""

from pathlib import Path

from domains.dataflow_ingest.event import normalize, payload_from_audit
from domains.dataflow_ingest.pack import failing_layer
from domains.dataflow_ingest.tools_gcp import default_gcp_calls
from incident_core.brain import Brain
from incident_core.signatures import match_signatures

from tests.fakes import FakePack, airflow_launch_failed_payload, airflow_orchestration, failed_quota_payload
from tests.test_graph import UNICODE_TRACEBACK, run

DATAFLOW_FAILED_LOG = """[2026-11-03, 10:09:40 UTC] {dataflow.py:1012} INFO - Google Cloud DataFlow job ingest-job-unicode is state: JOB_STATE_FAILED
[2026-11-03, 10:09:40 UTC] {taskinstance.py:3311} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/opt/python3.11/lib/python3.11/site-packages/airflow/providers/google/cloud/hooks/dataflow.py", line 455, in _check_dataflow_job_state
    raise AirflowException(f"Job failed with state {current_state}")
airflow.exceptions.AirflowException: Job failed with state JOB_STATE_FAILED"""


def top(cfg, payload):
    event = normalize(payload, {})
    matches = match_signatures(Brain(cfg["brain_dir"]).signatures, event["facts"], event["text"])
    return matches[0] if matches else None


def airflow_dataflow_failure(job_id="job-unicode"):
    payload = failed_quota_payload(job_id)
    payload["schema_version"] = "2"
    payload["file"]["name"] = "landing/customer_20261021.csv"
    payload["dataflow"]["error_messages"] = [{"time": "2026-11-03T10:09:30Z", "text": UNICODE_TRACEBACK}]
    payload["orchestration"] = airflow_orchestration(task_error=DATAFLOW_FAILED_LOG)
    return payload


def test_v2_event_is_normalized():
    event = normalize(airflow_launch_failed_payload(), {})
    assert event["facts"]["orchestrator"] == "airflow" and event["facts"]["failed_task"] == "run_dataflow"
    assert "flexTemplates:launch" in event["text"]  # the Airflow task error is failure signal
    assert event["links"]["airflow_dag_run"]


def test_event_without_job_id_uses_the_dag_run():
    payload = airflow_launch_failed_payload()
    payload["job"]["job_id"] = ""
    event = normalize(payload, {})
    assert event["incident_id"] == payload["orchestration"]["dag_run_id"] and event["job"]["job_id"] == ""


def test_default_calls_go_top_down():
    calls = [name for name, _ in default_gcp_calls(normalize(airflow_dataflow_failure(), {}))]
    assert calls[:3] == ["get_airflow_dag_run", "get_airflow_task_instances", "get_airflow_task_log"]
    assert calls.index("get_airflow_task_log") < calls.index("get_dataflow_errors")


def test_launch_failure_looks_for_the_dataflow_job():
    calls = default_gcp_calls(normalize(airflow_launch_failed_payload(), {}))
    assert ("find_dataflow_job", {"audit_job_id": "job-launch-1"}) in calls
    assert all(name != "get_dataflow_errors" for name, _ in calls)


def test_replay_of_a_composer_job():
    row = {"job_id": "j1", "status": "FAILED", "error_message": "Dataflow job 2026-11-03_abc ended in JOB_STATE_FAILED. x",
           "file_path": "gs://b/landing/x_20261015.csv", "service_name": "composer",
           "service_revision": "file__17__x_20261015.csv"}  # fmt: skip
    event = normalize(payload_from_audit(row, [], "p", "europe-west2", "dfa-composer"), {})
    assert event["orchestration"]["type"] == "airflow"
    assert event["orchestration"]["dag_run_id"] == "file__17__x_20261015.csv"
    assert event["dataflow"]["job_id"] == "2026-11-03_abc"
    # Replays don't know the failed task: a FAILED job reads run_dataflow's log.
    assert ("get_airflow_task_log", {"dag_run_id": "file__17__x_20261015.csv", "task_id": "run_dataflow"}) in default_gcp_calls(event)


def test_airflow_signatures(cfg):
    assert top(cfg, airflow_launch_failed_payload())["id"] == "dataflow_launch_rejected"
    zombie = airflow_launch_failed_payload()
    zombie["orchestration"]["task_error"] = "Detected zombie job: {'DAG Id': 'gcs_to_bq_dataflow', ...}"
    zombie["dataflow"]["launch_error"] = ""
    assert (top(cfg, zombie)["id"], top(cfg, zombie)["category"]) == ("airflow_zombie", "TRANSIENT_INFRA")
    timeout = airflow_launch_failed_payload()
    timeout["dataflow"]["launch_error"] = ""
    timeout["orchestration"]["task_error"] = "airflow.exceptions.AirflowTaskTimeout: Timeout, PID: 4211"
    assert top(cfg, timeout)["id"] == "airflow_task_timeout" and top(cfg, timeout)["confidence"] < 0.7  # never auto-retried
    dag_bug = airflow_launch_failed_payload()
    dag_bug["dataflow"]["launch_error"] = ""
    dag_bug["orchestration"]["task_error"] = ('Traceback (most recent call last):\n  File "/home/airflow/gcs/dags/gcs_to_bq_dataflow.py", '
                                              "line 120, in register_job\nKeyError: 'generation'")  # fmt: skip
    assert top(cfg, dag_bug)["id"] == "airflow_dag_code_error"


def test_failing_layer_rules():
    launch = normalize(airflow_launch_failed_payload(), {})
    assert failing_layer(launch, [{"kind": "find_dataflow_job", "data": {"exists": False}}]) == "AIRFLOW"
    dataflow = normalize(airflow_dataflow_failure(), {})
    assert failing_layer(dataflow, [{"kind": "get_dataflow_job", "data": {"state": "JOB_STATE_FAILED"}}]) == "DATAFLOW"
    recon = normalize(airflow_dataflow_failure(), {})
    recon["orchestration"]["failed_task"] = ""
    assert failing_layer(recon, [{"kind": "get_dataflow_job", "data": {"state": "JOB_STATE_DONE"}}]) == "PIPELINE"
    register = normalize(airflow_launch_failed_payload(), {})
    register["orchestration"]["failed_task"] = "register_job"
    assert failing_layer(register, []) == "AIRFLOW"


def test_launch_rejected_stops_at_the_airflow_layer(cfg):
    pack = FakePack(cfg)
    result, _ = run(cfg, pack, airflow_launch_failed_payload())

    rc = result["root_cause"]
    assert (rc["category"], rc["signature"]["id"]) == ("CONFIG", "dataflow_launch_rejected")
    assert pack.executed == []  # CONFIG: a human applies the fix, then approves the retry
    report = Path(result["report_paths"]["report_md"]).read_text()
    assert "## Investigation path" in report
    assert "| Failing layer | AIRFLOW |" in report
    assert "none created" in report


def test_dataflow_crash_is_traced_through_airflow(cfg):
    pack = FakePack(cfg)
    pack.airflow_log = DATAFLOW_FAILED_LOG
    pack.error_text = UNICODE_TRACEBACK
    result, _ = run(cfg, pack, airflow_dataflow_failure())

    rc = result["root_cause"]
    assert (rc["category"], rc["signature"]["id"]) == ("CODE_BUG", "encoding_error")
    report = Path(result["report_paths"]["report_md"]).read_text()
    assert "| Failing layer | DATAFLOW |" in report
    assert "| Airflow task | run_dataflow failed" in report
    assert "UnicodeDecodeError" in report
