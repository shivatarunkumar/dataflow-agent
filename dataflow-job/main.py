"""Entry point for the GCS -> BigQuery ingestion pipeline (local, DataflowRunner and flex template)."""

import logging
import uuid

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions

from pipeline.contracts import load_contracts
from pipeline.options import IngestOptions
from pipeline.pipeline import build_pipeline


def run(argv=None):
    options = PipelineOptions(argv)
    ingest = options.view_as(IngestOptions)
    gcp = options.view_as(GoogleCloudOptions)
    if not ingest.input_file:
        raise ValueError("--input_file is required")
    if not gcp.project:
        raise ValueError("--project is required")

    if not ingest.audit_job_id and ingest.trigger_type != "MANUAL":
        # Generating an id here would create a second job_metadata row next to the workflow's.
        raise ValueError(f"--audit_job_id is required when trigger_type={ingest.trigger_type}")
    job_id = ingest.audit_job_id or str(uuid.uuid4())
    logging.info("Ingesting %s as job_id=%s", ingest.input_file, job_id)

    with beam.Pipeline(options=options) as p:
        build_pipeline(
            p,
            input_file=ingest.input_file,
            job_id=job_id,
            contracts=load_contracts(ingest.contracts_dir),
            project=gcp.project,
            region=gcp.region,
            audit_dataset=ingest.audit_dataset,
            target_dataset=ingest.target_dataset,
            run={
                "trigger_type": ingest.trigger_type,
                "pubsub_message_id": ingest.pubsub_message_id,
                "file_generation": ingest.file_generation,
            },
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
