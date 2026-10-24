import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery


class LoadToTargetTable(beam.PTransform):
    """Loads valid rows into the contract's target table, only if every file check passed.

    The table and schema come from the FileMeta side input, so one pipeline serves every contract.
    Returns the WriteToBigQuery result (load/copy job ids are used to sequence reconciliation).
    """

    def __init__(self, project: str, dataset: str, meta, file_ok):
        super().__init__()
        self._project = project
        self._dataset = dataset
        self._meta = meta
        self._file_ok = file_ok

    def expand(self, rows):
        project, dataset = self._project, self._dataset
        return (
            rows
            | "GateOnValidation" >> beam.Filter(lambda _, ok: ok, ok=self._file_ok)
            | "WriteToBigQuery" >> WriteToBigQuery(
                table=lambda _, meta: f"{project}:{dataset}.{meta['contract'].target_table}",
                table_side_inputs=(self._meta,),
                schema=lambda _, meta: meta["contract"].bq_schema(),
                schema_side_inputs=(self._meta,),
                method=WriteToBigQuery.Method.FILE_LOADS,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )
