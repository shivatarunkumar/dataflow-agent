"""Pipeline graph.

  Create(file) ─ ValidateFileStructure ─┬─ meta (side input everywhere)
                                        └─ structure results ─────────────┐
  ReadFromText ─ IsDataLine ─ ParseRecord ─┬─ valid rows ─ Gate ─ BQ load │
                                           ├─ delimiter errors ─┐         │
                                           └─ type errors ──────┤         │
                       Count rows ──────────────────────────────┴─ content results
  all results ─ AllChecksPassed (gate) ; load jobs ─ ReconcileCounts ─ recon result
  all results ─ validation_metadata ; FinalizeJobMetadata ─ job_metadata MERGE
"""

import apache_beam as beam
from apache_beam.pvalue import AsList, AsSingleton

from pipeline.transforms.audit import FinalizeJobMetadata, WriteValidationMetadata
from pipeline.transforms.combiners import AllChecksPassedFn, SampleAndCountFn
from pipeline.transforms.file_checks import BuildContentChecks, IsDataLine, ValidateFileStructure
from pipeline.transforms.load import LoadToTargetTable
from pipeline.transforms.parse import ParseRecord
from pipeline.transforms.recon import ReconcileCounts


class ValidateAndParse(beam.PTransform):
    """All pre-load validation. No GCP calls, so it is unit-testable with local files.

    Returns a dict of PCollections:
      meta (1 FileMeta), rows (valid typed rows), results (pre-load ValidationResults),
      rows_in_file (1 int), delimiter_errors / type_errors (1 SampleAndCountFn output each),
      file_ok (1 bool).
    """

    def __init__(self, input_file: str, job_id: str, contracts: list):
        super().__init__()
        self._input_file = input_file
        self._job_id = job_id
        self._contracts = contracts

    def expand(self, pbegin):
        structure = (
            pbegin
            | "SeedFile" >> beam.Create([self._input_file])
            | "ValidateFileStructure" >> beam.ParDo(ValidateFileStructure(self._contracts)).with_outputs(
                ValidateFileStructure.RESULTS, main="meta"
            )
        )
        meta = structure["meta"]
        meta_si = AsSingleton(meta)

        data_lines = (
            pbegin
            | "ReadLines" >> beam.io.ReadFromText(self._input_file, skip_header_lines=1)
            | "DropBlankAndTrailer" >> beam.ParDo(IsDataLine(), meta=meta_si)
        )
        parsed = data_lines | "ParseRecord" >> beam.ParDo(ParseRecord(self._job_id), meta=meta_si).with_outputs(
            ParseRecord.DELIMITER_ERROR, ParseRecord.TYPE_ERROR, main="valid"
        )

        rows_in_file = data_lines | "CountDataRows" >> beam.combiners.Count.Globally()
        delimiter_errors = parsed[ParseRecord.DELIMITER_ERROR] | "AggDelimiterErrors" >> beam.CombineGlobally(
            SampleAndCountFn()
        )
        type_errors = parsed[ParseRecord.TYPE_ERROR] | "AggTypeErrors" >> beam.CombineGlobally(SampleAndCountFn())

        content_results = (
            pbegin
            | "ContentImpulse" >> beam.Create([None])
            | "BuildContentChecks" >> beam.ParDo(
                BuildContentChecks(),
                meta=meta_si,
                rows_in_file=AsSingleton(rows_in_file),
                delimiter_errors=AsSingleton(delimiter_errors),
                type_errors=AsSingleton(type_errors),
            )
        )
        results = (structure[ValidateFileStructure.RESULTS], content_results) | "FlattenPreLoad" >> beam.Flatten()
        file_ok = results | "AllChecksPassed" >> beam.CombineGlobally(AllChecksPassedFn())

        return {
            "meta": meta,
            "rows": parsed["valid"],
            "results": results,
            "rows_in_file": rows_in_file,
            "delimiter_errors": delimiter_errors,
            "type_errors": type_errors,
            "file_ok": file_ok,
        }


def build_pipeline(p, *, input_file, job_id, contracts, project, region, audit_dataset, target_dataset, run):
    v = p | "ValidateAndParse" >> ValidateAndParse(input_file, job_id, contracts)
    meta_si = AsSingleton(v["meta"])

    write_result = v["rows"] | "LoadToTargetTable" >> LoadToTargetTable(
        project, target_dataset, meta_si, AsSingleton(v["file_ok"])
    )

    recon = (
        p
        | "ReconImpulse" >> beam.Create([None])
        | "ReconcileCounts" >> beam.ParDo(
            ReconcileCounts(project, target_dataset, job_id),
            meta=meta_si,
            rows_in_file=AsSingleton(v["rows_in_file"]),
            file_ok=AsSingleton(v["file_ok"]),
            load_jobs=AsList(write_result.destination_load_jobid_pairs),
            copy_jobs=AsList(write_result.destination_copy_jobid_pairs),
        )
    )
    all_results = (v["results"], recon) | "FlattenAllResults" >> beam.Flatten()

    _ = all_results | "ValidationMetadata" >> WriteValidationMetadata(
        project, audit_dataset, target_dataset, job_id, meta_si
    )
    _ = (
        p
        | "FinalizeImpulse" >> beam.Create([None])
        | "FinalizeJobMetadata" >> beam.ParDo(
            FinalizeJobMetadata(project, region, audit_dataset, target_dataset, job_id, run),
            meta=meta_si,
            results=AsList(all_results),
            rows_in_file=AsSingleton(v["rows_in_file"]),
            delimiter_errors=AsSingleton(v["delimiter_errors"]),
            type_errors=AsSingleton(v["type_errors"]),
        )
    )
