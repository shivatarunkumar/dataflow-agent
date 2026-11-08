"""Unit tests for the DAGs' pure logic and the error event (no Airflow, no GCP). Run: make dags-test."""

import json
from datetime import datetime

from dfa_common.events import build_error_event, event_attributes
from dfa_common.logic import (archive_destination, decide_outcome, failure_message, file_event, first_failed_task,
                              ingest_run_id)  # fmt: skip

FINALIZE = {"eventType": "OBJECT_FINALIZE"}


def obj(name="landing/customer_20261015.csv", generation=1730612345678901):
    return {"bucket": "b", "name": name, "generation": generation, "size": "120", "timeCreated": "2026-11-03T10:00:00Z"}


def test_file_event_builds_deterministic_run_id_and_conf():
    event = file_event(obj(), FINALIZE, "m-1", 2)
    assert event["trigger_run_id"] == "file__1730612345678901__customer_20261015.csv"
    assert event["conf"]["generation"] == "1730612345678901"
    assert event["conf"]["size"] == 120 and event["conf"]["delivery_attempt"] == 2


def test_redelivery_maps_to_the_same_run_id():
    assert file_event(obj(), FINALIZE, "m-1", 1)["trigger_run_id"] == file_event(obj(), FINALIZE, "m-2", 2)["trigger_run_id"]


def test_new_generation_gets_a_new_run():
    assert file_event(obj(generation=1), FINALIZE, "m", 1)["trigger_run_id"] != file_event(obj(generation=2), FINALIZE, "m", 1)["trigger_run_id"]


def test_ignored_events():
    assert file_event(obj(), {"eventType": "OBJECT_DELETE"}, "m", 1) is None
    assert file_event(obj("landing/"), FINALIZE, "m", 1) is None
    assert file_event(obj("archive/2026/11/03/x.csv"), FINALIZE, "m", 1) is None


def test_run_id_is_airflow_safe():
    run_id = ingest_run_id("landing/sub dir/odd name (1).csv", "5")
    assert run_id == "file__5__sub_dir_odd_name__1_.csv"
    assert len(ingest_run_id("landing/" + "x" * 400, "5")) == 250


def test_outcome_success_and_rejected_stand():
    assert decide_outcome({"id": "d", "state": "JOB_STATE_DONE"}, "SUCCESS") == ("SUCCESS", None)
    assert decide_outcome({"id": "d", "state": "JOB_STATE_DONE"}, "REJECTED") == ("REJECTED", None)


def test_outcome_dataflow_failed():
    assert decide_outcome({"id": "d", "state": "JOB_STATE_FAILED"}, "IN_PROGRESS") == ("FAILED", "DATAFLOW_JOB_FAILED")
    assert decide_outcome({"id": "d", "state": "JOB_STATE_DONE"}, "IN_PROGRESS") == ("FAILED", "DATAFLOW_JOB_FAILED")


def test_outcome_no_dataflow_job_is_an_airflow_failure():
    assert decide_outcome({}, "IN_PROGRESS") == ("FAILED", "AIRFLOW_TASK_FAILED")


def test_failure_message_keeps_replayable_prefix():
    msg = failure_message({}, "failed", "Traceback ...\ngoogleapiclient.errors.HttpError: <HttpError 400 ...>\n")
    assert msg.startswith("Dataflow job  ended in NOT_STARTED. Airflow task run_dataflow failed.")
    assert msg.endswith("HttpError: <HttpError 400 ...>")


def test_first_failed_task_follows_dag_order():
    states = {"finalize": "failed", "run_dataflow": "failed", "register_job": "success"}
    assert first_failed_task(states) == "run_dataflow"
    assert first_failed_task({"finalize": "failed"}, exclude=("finalize",)) == ""


def test_archive_destination():
    now = datetime(2026, 11, 3)
    assert archive_destination("landing/a.csv", "SUCCESS", now) == "archive/2026/11/03/a.csv"
    assert archive_destination("landing/a.csv", "FAILED", now) == "invalid/2026/11/03/a.csv"


def test_error_event_v2_shape():
    event = build_error_event(
        job={"job_id": "j1", "error_code": "DATAFLOW_JOB_FAILED", "job_name": "customer_load"}, status="FAILED",
        file={"bucket": "b", "name": "landing/customer_20261021.csv"}, failed_checks=[],
        dataflow={"job_id": "2026-11-03_df", "state": "JOB_STATE_FAILED", "launch_error": "", "error_messages": []},
        orchestration={"dag_id": "gcs_to_bq_dataflow", "dag_run_id": "file__1__customer_20261021.csv",
                       "failed_task": "run_dataflow", "task_error": "boom", "tasks": [{"task_id": "run_dataflow", "start_date": datetime(2026, 11, 3)}]},
    )  # fmt: skip
    assert event["schema_version"] == "2" and event["event_type"] == "ingestion.job.failed"
    assert event["orchestration"]["type"] == "airflow"
    assert "2026-11-03_df" in event["dataflow"]["console_url"]
    assert event_attributes(event) == {"status": "FAILED", "error_code": "DATAFLOW_JOB_FAILED", "job_id": "j1",
                                       "job_name": "customer_load", "file_name": "landing/customer_20261021.csv",
                                       "orchestrator": "airflow"}  # fmt: skip
    json.dumps(event, default=str)  # serialisable as published

