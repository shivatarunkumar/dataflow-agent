import pytest

from pipeline import checks
from pipeline.contracts import find_contract, load_contracts

CONTRACTS = load_contracts()
CUSTOMER = find_contract("customer_20261015.csv", CONTRACTS)
ORDERS = find_contract("orders_20261015.csv", CONTRACTS)
CUSTOMER_HEADER = "customer_id,first_name,last_name,email,phone,country,signup_date,is_active"
NO_BAD_ROWS = {"count": 0, "samples": [], "rule_counts": {}}


@pytest.mark.parametrize(
    "file_name, expected",
    [("customer_20261015.csv", "customer"), ("orders_x.csv", "orders"), ("unknown_20261015.csv", None)],
)
def test_find_contract(file_name, expected):
    contract = find_contract(file_name, CONTRACTS)
    assert (contract.name if contract else None) == expected


@pytest.mark.parametrize(
    "file_name, passed, file_date",
    [
        ("customer_20261015.csv", True, "2026-10-15"),
        ("customer_20261332.csv", False, None),
        ("customer_2026-10-19.csv", False, None),
        ("customer_id_200.csv", False, None),
        ("customer_20261015.xlsx", False, None),
    ],
)
def test_check_file_name(file_name, passed, file_date):
    date, result = checks.check_file_name(file_name, CUSTOMER)
    assert (result.result == checks.PASS) is passed
    assert date == file_date


def test_check_header_pass_and_missing_column():
    assert checks.check_header(CUSTOMER_HEADER, CUSTOMER).result == checks.PASS
    result = checks.check_header(CUSTOMER_HEADER.replace(",phone", ""), CUSTOMER)
    assert result.result == checks.FAIL
    assert result.details["missing"] == ["phone"]


def test_check_delimiter_detects_pipe():
    result = checks.check_delimiter(CUSTOMER_HEADER.replace(",", "|"), CUSTOMER, 3, NO_BAD_ROWS)
    assert result.result == checks.FAIL
    assert result.details["detected_delimiter"] == "|"
    assert "HEADER_DELIMITER" in result.failed_checks


def test_check_delimiter_bad_rows():
    bad = {"count": 2, "samples": [{"line": "x"}], "rule_counts": {}}
    result = checks.check_delimiter(CUSTOMER_HEADER, CUSTOMER, 10, bad)
    assert result.failed_checks == ["ROW_FIELD_COUNT"]


@pytest.mark.parametrize(
    "trailer, rows, failed",
    [
        ("T|5|20261015", 5, []),
        ("T|6|20261015", 5, ["TRAILER_COUNT"]),
        ("T|5|20261016", 5, ["TRAILER_DATE"]),
        ("5,Ella,Taylor", 5, ["TRAILER_FORMAT"]),
        ("", 0, ["TRAILER_FORMAT"]),
    ],
)
def test_check_trailer(trailer, rows, failed):
    result = checks.check_trailer(trailer, "2026-10-15", rows, CUSTOMER)
    assert result.failed_checks == failed
    assert (result.result == checks.PASS) is (not failed)


def test_parse_row_types_and_errors():
    row, errors = checks.parse_row(
        ["1", "Ana", "Patel", "a@x.com", "", "UK", "2025-10-09", "true"], CUSTOMER
    )
    assert errors == []
    assert row["customer_id"] == 1 and row["phone"] is None and row["is_active"] is True

    _, errors = checks.parse_row(["", "Ana", "P", "a@x", "", "UK", "31/02/2026", "maybe"], CUSTOMER)
    assert errors == ["customer_id.NOT_NULL", "signup_date.TYPE", "is_active.TYPE"]


def test_parse_row_custom_date_format_and_numeric():
    row, errors = checks.parse_row(["5001", "10", "15/10/2026", "220.21", "INR", "PLACED"], ORDERS)
    assert errors == []
    assert row["order_date"] == "2026-10-15" and row["amount"] == "220.21"


def test_reconciliation():
    assert checks.check_reconciliation(5, 5, 5).result == checks.PASS
    assert checks.check_reconciliation(5, 5, 4).result == checks.FAIL
