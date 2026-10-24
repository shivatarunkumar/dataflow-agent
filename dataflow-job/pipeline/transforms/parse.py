import apache_beam as beam

from pipeline import checks


class ParseRecord(beam.DoFn):
    """Parses a data line against the contract.

    Main output: typed row dict with lineage columns.
    Tagged DELIMITER_ERROR: field count != column count. Tagged TYPE_ERROR: NOT_NULL / TYPE failures.
    """

    DELIMITER_ERROR = "delimiter_error"
    TYPE_ERROR = "type_error"

    def __init__(self, job_id: str):
        self._job_id = job_id

    def process(self, line, meta):
        contract = meta["contract"]
        if contract is None:
            return
        fields = checks.split_line(line, contract.delimiter)
        if len(fields) != len(contract.columns):
            yield beam.pvalue.TaggedOutput(
                self.DELIMITER_ERROR, {"line": line[:200], "field_count": len(fields)}
            )
            return

        row, errors = checks.parse_row(fields, contract)
        if errors:
            yield beam.pvalue.TaggedOutput(self.TYPE_ERROR, {"line": line[:200], "errors": errors})
            return

        row["_job_id"] = self._job_id
        row["_source_file"] = meta["file_path"]
        row["_loaded_at"] = meta["start_time"].isoformat()
        yield row
