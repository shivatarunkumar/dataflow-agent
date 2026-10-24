"""Data contracts: one YAML per entity describing the expected file and target table."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

DEFAULT_CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"
DEFAULT_TRAILER_PATTERN = r"^T\|(?P<count>\d+)\|(?P<date>\d{8})$"
DEFAULT_TRAILER_PREFIX = "T|"

LINEAGE_FIELDS = [
    {"name": "_job_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "_source_file", "type": "STRING", "mode": "NULLABLE"},
    {"name": "_loaded_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
]


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True
    format: Optional[str] = None


@dataclass(frozen=True)
class Contract:
    name: str
    version: str
    file_prefix: str
    file_name_pattern: str
    delimiter: str
    target_table: str
    columns: tuple
    load_mode: str = "append"
    trailer_pattern: str = DEFAULT_TRAILER_PATTERN
    trailer_prefix: str = DEFAULT_TRAILER_PREFIX
    extra: dict = field(default_factory=dict, compare=False, hash=False)

    @property
    def column_names(self) -> list:
        return [c.name for c in self.columns]

    def bq_schema(self) -> dict:
        fields = [
            {"name": c.name, "type": c.type, "mode": "NULLABLE" if c.nullable else "REQUIRED"}
            for c in self.columns
        ]
        return {"fields": fields + LINEAGE_FIELDS}

    def snapshot(self) -> dict:
        """JSON-serialisable view recorded as check_config in validation_metadata."""
        return {
            "name": self.name,
            "version": self.version,
            "file_name_pattern": self.file_name_pattern,
            "delimiter": self.delimiter,
            "trailer_pattern": self.trailer_pattern,
            "target_table": self.target_table,
            "columns": [c.__dict__ for c in self.columns],
        }


def _from_dict(d: dict) -> Contract:
    columns = tuple(Column(**c) for c in d.pop("columns"))
    known = {f for f in Contract.__dataclass_fields__} - {"columns", "extra"}
    kwargs = {k: d.pop(k) for k in list(d) if k in known}
    return Contract(columns=columns, extra=d, **kwargs)


def load_contracts(contracts_dir=None) -> list:
    path = Path(contracts_dir) if contracts_dir else DEFAULT_CONTRACTS_DIR
    contracts = [_from_dict(yaml.safe_load(p.read_text())) for p in sorted(path.glob("*.yaml"))]
    if not contracts:
        raise ValueError(f"No contracts found in {path}")
    return contracts


def find_contract(file_name: str, contracts: list) -> Optional[Contract]:
    """Map a file to its contract by '<file_prefix>_' (longest prefix wins)."""
    matches = [c for c in contracts if file_name.startswith(f"{c.file_prefix}_")]
    return max(matches, key=lambda c: len(c.file_prefix)) if matches else None
