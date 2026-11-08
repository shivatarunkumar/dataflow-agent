"""Parameterised BigQuery queries for the audit tables (job_metadata is only ever written with DML)."""

from dfa_common import config


def _client():
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    return BigQueryHook(use_legacy_sql=False).get_client(project_id=config.PROJECT_ID, location=config.REGION)


def run_query(sql: str, **params) -> list:
    """Runs a query with named parameters. params: name -> (bq_type, value). Returns the rows."""
    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter(k, t, v) for k, (t, v) in params.items()]
    )
    return list(_client().query(sql, job_config=job_config).result())
