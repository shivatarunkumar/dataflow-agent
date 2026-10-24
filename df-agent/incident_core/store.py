"""Incident records: append-only rows in BigQuery (agent memory + audit trail), mirrored to a local JSONL file."""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger(__name__)

HANDLED_STATUSES = ("AUTO_RESOLVED", "AWAITING_HUMAN", "APPROVED_BY_HUMAN", "NO_INCIDENT", "DUPLICATE")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class LocalIncidentStore:
    def __init__(self, state_dir: str):
        self.path = Path(state_dir) / "incidents.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _rows(self):
        if not self.path.exists():
            return []
        return [json.loads(line) for line in self.path.read_text().splitlines() if line.strip()]

    def record(self, row: dict):
        with self.path.open("a") as f:
            f.write(json.dumps(row, default=str) + "\n")

    def is_handled(self, incident_id: str) -> bool:
        return any(r["incident_id"] == incident_id and r.get("status") in HANDLED_STATUSES for r in self._rows())

    def retries_24h(self, subject: str) -> int:
        since = datetime.now(timezone.utc) - timedelta(hours=24)
        return sum(
            int(r.get("executed_retry_count") or 0)
            for r in self._rows()
            if r.get("subject") == subject and datetime.fromisoformat(r["created_at"]) >= since
        )


class BigQueryIncidentStore:
    """Append-only (streaming inserts); never updates or deletes rows."""

    def __init__(self, client, table: str):
        self.client = client
        self.table = table  # project.dataset.table

    def record(self, row: dict):
        payload = dict(row)
        for key in ("actions", "decisions", "executed_actions", "llm_models"):
            if key in payload and not isinstance(payload[key], str):
                payload[key] = json.dumps(payload[key], default=str)
        errors = self.client.insert_rows_json(self.table, [payload])
        if errors:
            raise RuntimeError(f"agent_incidents insert failed: {errors}")

    def _scalar(self, sql: str, **params):
        from google.cloud import bigquery

        config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in params.items()]
        )
        rows = list(self.client.query(sql, job_config=config).result())
        return rows[0][0] if rows else None

    def is_handled(self, incident_id: str) -> bool:
        statuses = ", ".join(f"'{s}'" for s in HANDLED_STATUSES)
        n = self._scalar(
            f"SELECT COUNT(*) FROM `{self.table}` WHERE incident_id = @id AND status IN ({statuses})", id=incident_id
        )
        return bool(n)

    def retries_24h(self, subject: str) -> int:
        n = self._scalar(
            f"SELECT COALESCE(SUM(executed_retry_count), 0) FROM `{self.table}` "
            "WHERE subject = @subject AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)",
            subject=subject,
        )
        return int(n or 0)


class CompositeStore:
    """Writes to every store; reads from the primary and falls back to the local mirror if it errors."""

    def __init__(self, primary, local: LocalIncidentStore):
        self.primary = primary
        self.local = local

    def record(self, row: dict):
        row = {"created_at": now_iso(), **row}
        self.local.record(row)
        if self.primary is not None:
            try:
                self.primary.record(row)
            except Exception as exc:
                log.error("could not record incident in BigQuery (kept locally): %s", exc)

    def _read(self, method: str, *args):
        if self.primary is not None:
            try:
                return getattr(self.primary, method)(*args)
            except Exception as exc:
                log.warning("BigQuery read %s failed, using local log: %s", method, exc)
        return getattr(self.local, method)(*args)

    def is_handled(self, incident_id: str) -> bool:
        return self._read("is_handled", incident_id)

    def retries_24h(self, subject: str) -> int:
        return self._read("retries_24h", subject)
