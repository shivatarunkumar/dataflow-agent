"""Pulls GCS OBJECT_FINALIZE notifications and triggers one ingestion DAG run per file generation.

The run_id is derived from the object generation + name, so a redelivered Pub/Sub message maps to the same run_id
and is skipped by Airflow instead of loading the file twice.
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor

from dfa_common import config
from dfa_common.logic import file_event


def to_file_events(pulled_messages, context):
    """ReceivedMessages -> one trigger per landed file (same file twice in one pull -> once)."""
    events = {}
    for received in pulled_messages:
        message = received.message
        event = file_event(json.loads(message.data.decode("utf-8")), dict(message.attributes), message.message_id,
                           received.delivery_attempt)  # fmt: skip
        if event:
            events[event["trigger_run_id"]] = event
    return list(events.values())


with DAG(
    dag_id=config.LISTENER_DAG_ID,
    description="Pull GCS file notifications from Pub/Sub and trigger ingestion per file.",
    schedule="*/2 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    default_args={"retries": 2, "retry_delay": timedelta(seconds=20)},
    tags=["ingestion", "pubsub"],
) as dag:
    # Messages are acked once pulled; each file then has its own tracked DAG run.
    pull_notifications = PubSubPullSensor(
        task_id="pull_file_notifications",
        project_id=config.PROJECT_ID,
        subscription=config.SUBSCRIPTION,
        max_messages=10,
        ack_messages=True,
        messages_callback=to_file_events,
        poke_interval=15,
        timeout=100,
        soft_fail=True,  # no messages -> skipped, not failed
    )

    TriggerDagRunOperator.partial(
        task_id="trigger_ingestion",
        trigger_dag_id=config.INGEST_DAG_ID,
        wait_for_completion=False,
        skip_when_already_exists=True,  # duplicate delivery of the same generation
    ).expand_kwargs(pull_notifications.output)
