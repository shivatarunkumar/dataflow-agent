"""File-level validation: contract match, file name, header (structure) and delimiter, trailer,
data types (content, computed once row-level outputs have been aggregated)."""

import apache_beam as beam
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems

from pipeline import checks
from pipeline.contracts import find_contract

TAIL_BYTES = 64 * 1024


def read_file_edges(path: str):
    """Returns (header_line, last_non_empty_line, size_bytes) without reading the whole file."""
    size = FileSystems.match([path])[0].metadata_list[0].size_in_bytes
    with FileSystems.open(path, compression_type=CompressionTypes.UNCOMPRESSED) as f:
        first = f.readline()
        f.seek(max(0, size - TAIL_BYTES))
        tail = f.read()
    header = first.decode("utf-8-sig", errors="replace").rstrip("\r\n")
    lines = [line for line in tail.decode("utf-8", errors="replace").splitlines() if line.strip()]
    return header, (lines[-1] if lines else ""), size


class ValidateFileStructure(beam.DoFn):
    """Input: file path. Main output: one FileMeta dict. Tagged output RESULTS: ValidationResults."""

    RESULTS = "results"

    def __init__(self, contracts):
        self._contracts = contracts

    def process(self, path):
        file_name = path.rsplit("/", 1)[-1]
        contract = find_contract(file_name, self._contracts)
        yield beam.pvalue.TaggedOutput(
            self.RESULTS,
            checks.check_contract_match(file_name, contract, [c.file_prefix for c in self._contracts]),
        )

        header_line, last_line, size = read_file_edges(path)
        file_date = None
        if contract:
            file_date, name_result = checks.check_file_name(file_name, contract)
            yield beam.pvalue.TaggedOutput(self.RESULTS, name_result)
            yield beam.pvalue.TaggedOutput(self.RESULTS, checks.check_header(header_line, contract))

        yield {
            "file_path": path,
            "file_name": file_name,
            "source_bucket": path[5:].split("/", 1)[0] if path.startswith("gs://") else None,
            "file_size_bytes": size,
            "file_format": file_name.rsplit(".", 1)[-1].lower() if "." in file_name else None,
            "file_date": file_date,
            "contract": contract,
            "header_line": header_line,
            "trailer_line": last_line,
            "trailer_count": checks.parse_trailer_count(last_line, contract) if contract else None,
            "start_time": checks.utc_now(),
        }


class IsDataLine(beam.DoFn):
    """Drops blank lines and the trailer record (header is skipped by ReadFromText)."""

    def process(self, line, meta):
        contract = meta["contract"]
        if not line.strip():
            return
        if contract and line.startswith(contract.trailer_prefix):
            return
        yield line


class BuildContentChecks(beam.DoFn):
    """Runs once (on an impulse) after row-level aggregates are available as side inputs."""

    def process(self, _, meta, rows_in_file, delimiter_errors, type_errors):
        contract = meta["contract"]
        if contract is None:
            return
        yield checks.check_delimiter(meta["header_line"], contract, rows_in_file, delimiter_errors)
        yield checks.check_trailer(meta["trailer_line"], meta["file_date"], rows_in_file, contract)
        yield checks.check_data_types(rows_in_file, type_errors, contract)
