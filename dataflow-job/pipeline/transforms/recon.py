import apache_beam as beam

from pipeline import checks


class ReconcileCounts(beam.DoFn):
    """Post-load reconciliation: file data rows == trailer count == rows in BigQuery for this job_id.

    load_jobs / copy_jobs are the (destination, JobReference) pairs from WriteToBigQuery; consuming
    them as side inputs sequences this step after the load is triggered, and we wait for each job
    to finish before counting.
    """

    def __init__(self, project: str, dataset: str, job_id: str):
        self._project = project
        self._dataset = dataset
        self._job_id = job_id
        self._client = None

    def setup(self):
        from google.cloud import bigquery

        self._client = bigquery.Client(project=self._project)

    def process(self, _, meta, rows_in_file, file_ok, load_jobs, copy_jobs):
        contract = meta["contract"]
        if contract is None or not file_ok:
            yield checks.skipped_reconciliation("file rejected by pre-load validation")
            return

        job_ids = []
        for _, ref in list(load_jobs) + list(copy_jobs):
            self._client.get_job(ref.jobId, project=ref.projectId, location=ref.location).result()
            job_ids.append(ref.jobId)

        from google.cloud import bigquery

        table = f"`{self._project}.{self._dataset}.{contract.target_table}`"
        query = f"SELECT COUNT(*) AS n FROM {table} WHERE _job_id = @job_id"
        config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", self._job_id)]
        )
        rows_in_bq = next(iter(self._client.query(query, job_config=config).result())).n

        result = checks.check_reconciliation(rows_in_file, meta["trailer_count"], rows_in_bq)
        result.details["bq_job_ids"] = job_ids
        yield result
