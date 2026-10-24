from apache_beam.options.pipeline_options import PipelineOptions


class IngestOptions(PipelineOptions):
    """Custom options for the ingestion pipeline (also exposed as flex template parameters)."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input_file", help="gs:// URI (or local path for DirectRunner) of the file to ingest.")
        # Not named --job_id: that name did not reach the pipeline through the flex template launch.
        parser.add_argument(
            "--audit_job_id", default=None, help="job_metadata.job_id registered by the workflow; generated for MANUAL runs."
        )
        parser.add_argument("--file_generation", default=None, help="GCS object generation.")
        parser.add_argument("--pubsub_message_id", default=None, help="Pub/Sub message that triggered the run.")
        parser.add_argument("--trigger_type", default="MANUAL", help="PUBSUB or MANUAL.")
        parser.add_argument("--audit_dataset", default="file_audit", help="Dataset with the audit tables.")
        parser.add_argument("--target_dataset", default="raw", help="Dataset with the target tables.")
        parser.add_argument("--contracts_dir", default=None, help="Directory of contract YAMLs; defaults to bundled contracts.")
