"""Runs the ValidateAndParse composite on local files with the DirectRunner (no GCP access)."""

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from pipeline.contracts import load_contracts
from pipeline.pipeline import ValidateAndParse

CONTRACTS = load_contracts()
HEADER = "customer_id,first_name,last_name,email,phone,country,signup_date,is_active"
ROWS = [
    "1,Ana,Patel,ana@example.com,+44 7700 900001,UK,2025-10-09,true",
    "2,Ben,Khan,ben@example.com,,US,2025-04-15,false",
]


def write_file(tmp_path, name, lines):
    path = tmp_path / name
    path.write_text("\r\n".join(lines) + "\r\n")
    return str(path)


def run_checks(path):
    """Returns PCollections of (check_name, result) pairs, row ids and file_ok."""
    p = TestPipeline()
    out = p | ValidateAndParse(path, "job-1", CONTRACTS)
    results = out["results"] | "Pairs" >> beam.Map(lambda r: (r.check_name, r.result))
    row_ids = out["rows"] | "Ids" >> beam.Map(lambda r: r["customer_id"])
    return p, results, row_ids, out["file_ok"]


def test_valid_file(tmp_path):
    path = write_file(tmp_path, "customer_20261015.csv", [HEADER, *ROWS, "T|2|20261015"])
    p, results, row_ids, file_ok = run_checks(path)
    assert_that(
        results,
        equal_to([(c, "PASS") for c in ["contract_match", "file_name", "header", "delimiter", "trailer", "data_type"]]),
        label="AllPass",
    )
    assert_that(row_ids, equal_to([1, 2]), label="Rows")
    assert_that(file_ok, equal_to([True]), label="FileOk")
    p.run()


@pytest.mark.parametrize(
    "name, lines, failing",
    [
        ("customer_20261015.csv", [HEADER, *ROWS, "T|3|20261015"], "trailer"),
        ("customer_20261015.csv", [HEADER, *ROWS], "trailer"),
        ("customer_20261015.csv", [HEADER.replace(",phone", ""), *ROWS, "T|2|20261015"], "header"),
        ("customer_20261015.csv", [HEADER.replace(",", "|"), *[r.replace(",", "|") for r in ROWS], "T|2|20261015"], "delimiter"),
        ("customer_20261015.csv", [HEADER, ROWS[0], "x,Ben,K,b@x,,US,bad-date,true", "T|2|20261015"], "data_type"),
        ("customer_2026-10-15.csv", [HEADER, *ROWS, "T|2|20261015"], "file_name"),
        ("unknown_20261015.csv", ["id,value", "1,a", "T|1|20261015"], "contract_match"),
    ],
)
def test_rejected_files(tmp_path, name, lines, failing):
    path = write_file(tmp_path, name, lines)
    p, results, _, file_ok = run_checks(path)
    failed = results | beam.Filter(lambda pair: pair[1] == "FAIL") | beam.Keys()
    assert_that(failed | "HasFailing" >> beam.Map(lambda c: c == failing) | beam.CombineGlobally(any),
                equal_to([True]), label="ExpectedFailure")
    assert_that(file_ok, equal_to([False]), label="FileRejected")
    p.run()
