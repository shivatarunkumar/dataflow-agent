"""Pure validation logic. Transforms in pipeline/transforms call these; they never do I/O."""

import csv
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

from pipeline.contracts import Column, Contract

PASS, FAIL, SKIPPED = "PASS", "FAIL", "SKIPPED"
ERROR = "ERROR"
PRE_LOAD, POST_LOAD = "PRE_LOAD", "POST_LOAD"

DELIMITER_CANDIDATES = [",", "|", ";", "\t"]
MAX_SAMPLES = 10


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ValidationResult:
    check_name: str
    check_type: str
    stage: str
    result: str
    summary: str
    records_evaluated: Optional[int] = None
    records_failed: Optional[int] = None
    checks_evaluated: int = 1
    failed_checks: list = field(default_factory=list)
    details: dict = field(default_factory=dict)
    check_config: dict = field(default_factory=dict)
    validated_at: datetime = field(default_factory=utc_now)

    @property
    def severity(self) -> Optional[str]:
        return ERROR if self.result == FAIL else None

    @property
    def is_blocking(self) -> bool:
        """Every check is ERROR severity: any failure rejects the file."""
        return self.result == FAIL


def _result(passed: bool) -> str:
    return PASS if passed else FAIL


def split_line(line: str, delimiter: str) -> list:
    return next(csv.reader([line], delimiter=delimiter))


def detect_delimiter(line: str) -> Optional[str]:
    counts = {d: line.count(d) for d in DELIMITER_CANDIDATES}
    best = max(counts, key=counts.get)
    return best if counts[best] else None


# --- File-level checks ------------------------------------------------------------------


def check_contract_match(file_name: str, contract: Optional[Contract], known_prefixes: list) -> ValidationResult:
    return ValidationResult(
        check_name="contract_match",
        check_type="CONTRACT_MATCH",
        stage=PRE_LOAD,
        result=_result(contract is not None),
        summary=(
            f"Matched contract '{contract.name}' v{contract.version}"
            if contract
            else f"No contract matches '{file_name}' (expected prefix one of {known_prefixes})"
        ),
        failed_checks=[] if contract else ["CONTRACT_MATCH"],
        check_config={"known_prefixes": known_prefixes},
    )


def check_file_name(file_name: str, contract: Contract):
    """Returns (file_date as ISO string or None, ValidationResult)."""
    failures, file_date = [], None
    match = re.match(contract.file_name_pattern, file_name)
    if not match:
        failures.append("FILE_NAME_PATTERN")
    else:
        try:
            file_date = datetime.strptime(match.group("file_date"), "%Y%m%d").date().isoformat()
        except ValueError:
            failures.append("FILE_NAME_DATE")

    return file_date, ValidationResult(
        check_name="file_name",
        check_type="FILE_FORMAT",
        stage=PRE_LOAD,
        result=_result(not failures),
        summary=(
            f"File name valid, business date {file_date}"
            if not failures
            else f"File name '{file_name}' does not match {contract.file_name_pattern} with a valid YYYYMMDD date"
        ),
        checks_evaluated=2,
        failed_checks=failures,
        details={"file_name": file_name, "file_date": file_date},
        check_config={"file_name_pattern": contract.file_name_pattern},
    )


def check_header(header_line: str, contract: Contract) -> ValidationResult:
    actual = [c.strip() for c in split_line(header_line, contract.delimiter)] if header_line else []
    expected = contract.column_names
    passed = actual == expected
    details = {"expected": expected, "actual": actual}
    if not passed:
        details["missing"] = [c for c in expected if c not in actual]
        details["unexpected"] = [c for c in actual if c not in expected]
        details["order_mismatch"] = not details["missing"] and not details["unexpected"]
    return ValidationResult(
        check_name="header",
        check_type="FILE_FORMAT",
        stage=PRE_LOAD,
        result=_result(passed),
        summary=(
            f"Header matches {len(expected)} contract columns"
            if passed
            else f"Header mismatch: missing={details['missing']}, unexpected={details['unexpected']}"
        ),
        failed_checks=[] if passed else ["HEADER_MATCH"],
        details=details,
        check_config={"columns": expected, "delimiter": contract.delimiter},
    )


def check_delimiter(header_line: str, contract: Contract, rows_evaluated: int, bad_rows: dict) -> ValidationResult:
    """bad_rows is the SampleCombineFn output for rows whose field count != column count."""
    expected_fields = len(contract.columns)
    header_fields = len(split_line(header_line, contract.delimiter)) if header_line else 0
    detected = detect_delimiter(header_line or "")
    failures = []
    if detected != contract.delimiter or header_fields != expected_fields:
        failures.append("HEADER_DELIMITER")
    if bad_rows["count"]:
        failures.append("ROW_FIELD_COUNT")
    return ValidationResult(
        check_name="delimiter",
        check_type="FILE_FORMAT",
        stage=PRE_LOAD,
        result=_result(not failures),
        summary=(
            f"Delimiter '{contract.delimiter}' valid for header and {rows_evaluated} rows"
            if not failures
            else f"Delimiter check failed: detected '{detected}', expected '{contract.delimiter}'; "
            f"{bad_rows['count']} of {rows_evaluated} rows have != {expected_fields} fields"
        ),
        records_evaluated=rows_evaluated,
        records_failed=bad_rows["count"],
        checks_evaluated=2,
        failed_checks=failures,
        details={
            "expected_delimiter": contract.delimiter,
            "detected_delimiter": detected,
            "header_field_count": header_fields,
            "expected_field_count": expected_fields,
            "samples": bad_rows["samples"],
        },
        check_config={"delimiter": contract.delimiter},
    )


def check_trailer(trailer_line: str, file_date: Optional[str], data_rows: int, contract: Contract) -> ValidationResult:
    """Trailer must be the last line: T|<count>|<YYYYMMDD>, count == data rows, date == file date."""
    config = {"trailer_pattern": contract.trailer_pattern}
    match = re.match(contract.trailer_pattern, trailer_line or "")
    if not match:
        return ValidationResult(
            check_name="trailer",
            check_type="FILE_FORMAT",
            stage=PRE_LOAD,
            result=FAIL,
            summary=f"Trailer missing or malformed; last line was '{(trailer_line or '')[:80]}'",
            records_evaluated=data_rows,
            checks_evaluated=3,
            failed_checks=["TRAILER_FORMAT"],
            details={"last_line": (trailer_line or "")[:200]},
            check_config=config,
        )

    trailer_count, trailer_date = int(match.group("count")), match.group("date")
    expected_date = file_date.replace("-", "") if file_date else None
    failures = []
    if expected_date and trailer_date != expected_date:
        failures.append("TRAILER_DATE")
    if trailer_count != data_rows:
        failures.append("TRAILER_COUNT")
    return ValidationResult(
        check_name="trailer",
        check_type="FILE_FORMAT",
        stage=PRE_LOAD,
        result=_result(not failures),
        summary=(
            f"Trailer valid: {trailer_count} rows, date {trailer_date}"
            if not failures
            else f"Trailer mismatch: count {trailer_count} vs {data_rows} data rows, "
            f"date {trailer_date} vs file date {expected_date}"
        ),
        records_evaluated=data_rows,
        checks_evaluated=3,
        failed_checks=failures,
        details={
            "trailer_count": trailer_count,
            "data_rows": data_rows,
            "trailer_date": trailer_date,
            "file_date": expected_date,
        },
        check_config=config,
    )


def check_data_types(rows_evaluated: int, bad_rows: dict, contract: Contract) -> ValidationResult:
    """bad_rows is the SampleCombineFn output for rows failing NOT_NULL / TYPE parsing."""
    failed = bad_rows["count"]
    rule_counts = bad_rows.get("rule_counts", {})
    return ValidationResult(
        check_name="data_type",
        check_type="DATA_CONTRACT",
        stage=PRE_LOAD,
        result=_result(failed == 0),
        summary=(
            f"All {rows_evaluated} rows match column types"
            if not failed
            else f"{failed} of {rows_evaluated} rows failed: "
            + ", ".join(f"{k} ({v} rows)" for k, v in sorted(rule_counts.items()))
        ),
        records_evaluated=rows_evaluated,
        records_failed=failed,
        checks_evaluated=len(contract.columns),
        failed_checks=sorted(rule_counts),
        details={"rule_counts": rule_counts, "samples": bad_rows["samples"]},
        check_config={"columns": [c.__dict__ for c in contract.columns]},
    )


def check_reconciliation(rows_in_file: int, trailer_count: Optional[int], rows_in_bq: int) -> ValidationResult:
    expected = [rows_in_file] + ([trailer_count] if trailer_count is not None else [])
    passed = all(n == rows_in_bq for n in expected)
    return ValidationResult(
        check_name="recon_row_count",
        check_type="RECONCILIATION",
        stage=POST_LOAD,
        result=_result(passed),
        summary=f"file rows={rows_in_file}, trailer={trailer_count}, bigquery rows={rows_in_bq}",
        records_evaluated=rows_in_file,
        records_failed=abs(rows_in_file - rows_in_bq),
        failed_checks=[] if passed else ["ROW_COUNT_MISMATCH"],
        details={"rows_in_file": rows_in_file, "trailer_count": trailer_count, "rows_in_bq": rows_in_bq},
    )


def skipped_reconciliation(reason: str) -> ValidationResult:
    return ValidationResult(
        check_name="recon_row_count",
        check_type="RECONCILIATION",
        stage=POST_LOAD,
        result=SKIPPED,
        summary=f"Skipped: {reason}",
        checks_evaluated=0,
    )


def parse_trailer_count(trailer_line: str, contract: Contract) -> Optional[int]:
    match = re.match(contract.trailer_pattern, trailer_line or "")
    return int(match.group("count")) if match else None


# --- Row parsing ------------------------------------------------------------------------

_TRUE = {"true", "t", "1", "y", "yes"}
_FALSE = {"false", "f", "0", "n", "no"}


def convert_value(raw: str, column: Column):
    """Convert a raw string to a BigQuery JSON-load compatible value; raises ValueError."""
    t = column.type.upper()
    if t == "STRING":
        return raw
    if t in ("INTEGER", "INT64"):
        return int(raw)
    if t in ("NUMERIC", "BIGNUMERIC"):
        value = Decimal(raw)
        if not value.is_finite():
            raise ValueError(f"non-finite numeric {raw}")
        return str(value)
    if t in ("FLOAT", "FLOAT64"):
        return float(raw)
    if t in ("BOOLEAN", "BOOL"):
        low = raw.lower()
        if low in _TRUE:
            return True
        if low in _FALSE:
            return False
        raise ValueError(f"not a boolean: {raw}")
    if t == "DATE":
        return datetime.strptime(raw, column.format or "%Y-%m-%d").date().isoformat()
    if t == "TIMESTAMP":
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.isoformat()
    raise ValueError(f"unsupported type {column.type}")


def parse_row(fields: list, contract: Contract):
    """Returns (row dict, list of 'column.RULE' errors)."""
    row, errors = {}, []
    for column, raw in zip(contract.columns, fields):
        value = raw if column.type.upper() == "STRING" else raw.strip()
        if value == "":
            row[column.name] = None
            if not column.nullable:
                errors.append(f"{column.name}.NOT_NULL")
            continue
        try:
            row[column.name] = convert_value(value, column)
        except (ValueError, ArithmeticError):
            errors.append(f"{column.name}.TYPE")
    return row, errors


def iso(value) -> Optional[str]:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value
