"""Generate the mock files used to test the file processor end to end.

Run from the repo root:  .venv/bin/python mockdata/generate.py
Output is deterministic, so re-running produces identical files.
"""

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from openpyxl import Workbook

OUT = Path(__file__).parent
rng = random.Random(42)

FIRST = ["Ana", "Ben", "Chloe", "Dev", "Ella", "Farah", "George", "Hana", "Ivan", "Jade",
         "Kiran", "Liam", "Maya", "Noah", "Olivia", "Priya", "Quinn", "Ravi", "Sara", "Tom"]  # fmt: skip
LAST = [
    "Smith",
    "Patel",
    "Jones",
    "Khan",
    "Brown",
    "Singh",
    "Taylor",
    "Wilson",
    "Evans",
    "Shah",
]
COUNTRIES = ["UK", "US", "IN", "CA", "AU", "DE", "FR"]

CUSTOMER_HEADER = ["customer_id", "first_name", "last_name", "email", "phone", "country", "signup_date",
                   "is_active"]  # fmt: skip
USER_HEADER = [
    "user_id",
    "username",
    "email",
    "customer_id",
    "role",
    "created_at",
    "last_login_at",
]
ORDER_HEADER = ["order_id", "customer_id", "order_date", "amount", "currency", "status"]


def write_csv(
    name: str,
    header: list[str],
    rows: list[list],
    trailer: str | None = "auto",
    delimiter: str = ",",
    encoding: str = "utf-8",
) -> None:
    """Writes header, rows and a trailer 'T|<count>|<YYYYMMDD>' (date taken from the file name).

    trailer="auto" builds the correct trailer, None omits it, any other string is written as-is.
    """
    if trailer == "auto":
        trailer = f"T|{len(rows)}|{name.rsplit('_', 1)[-1].split('.')[0]}"
    with (OUT / name).open("w", newline="", encoding=encoding) as f:
        w = csv.writer(f, delimiter=delimiter)
        w.writerow(header)
        w.writerows(rows)
        if trailer is not None:
            f.write(trailer + "\r\n")
    print(f"  {name:28} {len(rows):3} rows  trailer={trailer}")


def customers(n: int, start_id: int = 1) -> list[list]:
    rows = []
    for i in range(start_id, start_id + n):
        first, last = FIRST[(i - 1) % len(FIRST)], rng.choice(LAST)
        rows.append([
            i,
            first,
            last,
            f"{first.lower()}.{last.lower()}{i}@example.com",
            f"+44 7700 {900000 + i}" if i % 4 else "",  # some customers have no phone
            rng.choice(COUNTRIES),
            (date(2025, 1, 1) + timedelta(days=rng.randint(0, 600))).isoformat(),
            rng.choice(["true", "false"]),
        ])  # fmt: skip
    return rows


def users(n: int) -> list[list]:
    rows = []
    for i in range(1, n + 1):
        first = FIRST[(i - 1) % len(FIRST)].lower()
        created = datetime(2025, 3, 1, 9, 0) + timedelta(
            days=rng.randint(0, 400), minutes=rng.randint(0, 600)
        )
        last_login = (
            created + timedelta(days=rng.randint(1, 100)) if i % 3 else None
        )  # some never logged in
        rows.append([
            100 + i,
            f"{first}_{i}",
            f"{first}{i}@example.com",
            i,
            rng.choice(["admin", "member", "member", "viewer"]),
            created.strftime("%Y-%m-%dT%H:%M:%SZ"),
            last_login.strftime("%Y-%m-%dT%H:%M:%SZ") if last_login else "",
        ])  # fmt: skip
    return rows


def orders(n: int, start_id: int, day: date) -> list[list]:
    return [
        [
            start_id + i,
            rng.randint(1, 20),
            day.strftime("%d/%m/%Y"),  # orders contract uses DD/MM/YYYY
            f"{rng.uniform(5, 500):.2f}",
            rng.choice(["GBP", "USD", "EUR", "INR"]),
            rng.choice(["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]),
        ]
        for i in range(n)
    ]


def main() -> None:
    print("Valid files (expect SUCCESS, every CSV ends with trailer T|<count>|<YYYYMMDD>):")
    write_csv("customer_20261015.csv", CUSTOMER_HEADER, customers(20))
    write_csv("user_20261015.csv", USER_HEADER, users(15))
    write_csv("orders_20261015.csv", ORDER_HEADER, orders(25, 5001, date(2026, 10, 15)))

    # Only a WARNING-severity rule fails (phone format): loads, warning recorded.
    warn = customers(5, start_id=21)
    warn[1][4] = "call me"
    warn[3][4] = "12"
    write_csv("customer_20261017.csv", CUSTOMER_HEADER, warn)

    # Excel path, with real Excel date cells (converted to ISO, accepted alongside DD/MM/YYYY).
    wb = Workbook()
    ws = wb.active
    ws.title = "orders"
    ws.append(ORDER_HEADER)
    for row in orders(10, 6001, date(2026, 10, 16)):
        row[2] = datetime(2026, 10, 16)
        row[3] = float(row[3])
        ws.append(row)
    wb.save(OUT / "orders_20261016.xlsx")
    print(f"  {'orders_20261016.xlsx':28} {10:3} rows")

    print("Invalid files (expect REJECTED):")
    bad = customers(10, start_id=31)
    bad[1][3] = "not-an-email"  # REGEX
    bad[3][0] = ""  # NOT_NULL customer_id
    bad[5][0] = bad[4][0]  # UNIQUE customer_id
    bad[6][5] = "ZZ"  # ALLOWED_VALUES country
    bad[7][6] = "31/02/2026"  # TYPE signup_date
    bad[8][7] = "maybe"  # TYPE is_active
    write_csv("customer_20261016.csv", CUSTOMER_HEADER, bad)

    # HEADER_MATCH: the `role` column is missing.
    no_role = [r[:4] + r[5:] for r in users(5)]
    write_csv("user_20261016.csv", [c for c in USER_HEADER if c != "role"], no_role)

    # CONTRACT_MATCH: no contract matches this name.
    write_csv("unknown_20261015.csv", ["id", "value"], [[1, "a"], [2, "b"]])

    # TRAILER: count says 7 but the file has 5 data rows.
    write_csv("customer_20261018.csv", CUSTOMER_HEADER, customers(5, start_id=41), trailer="T|7|20261018")

    # TRAILER: no trailer record at all.
    write_csv("user_20261020.csv", USER_HEADER, users(4), trailer=None)

    # DELIMITER (and HEADER): pipe-delimited instead of comma.
    write_csv("orders_20261018.csv", ORDER_HEADER, orders(5, 7001, date(2026, 10, 18)), delimiter="|")

    print("Failing file (expect FAILED -> df-agent CODE_BUG incident):")
    # Latin-1 encoded: header/trailer/name checks pass, but the pipeline reads rows as UTF-8, so the Dataflow
    # job crashes with UnicodeDecodeError on 'José' / 'Müller' -> workflow marks the job FAILED.
    latin = customers(3, start_id=61)
    latin[0][1] = "José"
    latin[1][2] = "Müller"
    write_csv("customer_20261021.csv", CUSTOMER_HEADER, latin, encoding="latin-1")

    # FILE_NAME: date in the name is not YYYYMMDD.
    write_csv("customer_2026-10-19.csv", CUSTOMER_HEADER, customers(3, start_id=51), trailer="T|3|20261019")


if __name__ == "__main__":
    main()
