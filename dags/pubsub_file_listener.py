"""Listens to GCS OBJECT_FINALIZE notifications and triggers one ingestion DAG run per file."""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor

from dfa_common import config


def to_file_events(pulled_messages, context):
    """Turns ReceivedMessages into DAG run confs, ignoring non-finalize events and folder markers."""
    events = []
    for received in pulled_messages:
        message = received.message
        attributes = dict(message.attributes)
        if attributes.get("eventType") != "OBJECT_FINALIZE":
            continue
        obj = json.loads(message.data.decode("utf-8"))
        name = obj["name"]
        if name.endswith("/") or not name.startswith(config.LANDING_PREFIX):
            continue
        events.append({
            "bucket": obj["bucket"],
            "name": name,
            "generation": str(obj.get("generation")),
            "size": int(obj.get("size", 0)),
            "time_created": obj.get("timeCreated"),
            "message_id": message.message_id,
            "subscription": config.SUBSCRIPTION,
            "delivery_attempt": received.delivery_attempt or None,
        })  # fmt: skip
    return events


with DAG(
    dag_id="pubsub_file_listener",
    description="Pull GCS file notifications from Pub/Sub and trigger ingestion per file.",
    schedule="*/2 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
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
        soft_fail=True,  # no messages -> skip, not fail
    )

    TriggerDagRunOperator.partial(
        task_id="trigger_ingestion",
        trigger_dag_id=config.INGEST_DAG_ID,
        wait_for_completion=False,
    ).expand(conf=pull_notifications.output)
