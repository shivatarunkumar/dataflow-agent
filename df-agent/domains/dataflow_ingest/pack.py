"""DomainPack for the GCS -> Dataflow -> BigQuery ingestion pipeline."""

from pathlib import Path

from incident_core.domain import DomainPack, Gatherer
from incident_core.store import BigQueryIncidentStore

from domains.dataflow_ingest import event as event_mod
from domains.dataflow_ingest.clients import GcpClients
from domains.dataflow_ingest.executor import retry_file
from domains.dataflow_ingest.tools_audit import build_audit_tools, default_audit_calls
from domains.dataflow_ingest.tools_gcp import build_gcp_tools, default_gcp_calls

REPO_ROOT = Path(__file__).resolve().parents[3]  # dataflow-agent/


def _object_name(gs_path: str, bucket: str):
    prefix = f"gs://{bucket}/"
    return gs_path[len(prefix):] if gs_path and gs_path.startswith(prefix) else None


class DataflowIngestPack(DomainPack):
    name = "dataflow_ingest"

    def __init__(self, cfg: dict, clients=None):
        self.cfg = cfg
        self.gcp = cfg["gcp"]
        self.clients = clients or GcpClients(self.gcp)
        self.store = None
        self._audit_tools = build_audit_tools(self.clients, self.gcp)
        self._gcp_tools = build_gcp_tools(self.clients, self.gcp)

    # --- wiring -------------------------------------------------------------------------------------
    def attach_store(self, store):
        self.store = store

    def incident_store(self):
        table = f"{self.gcp['project']}.{self.gcp['audit_dataset']}.{self.gcp['incidents_table']}"
        return BigQueryIncidentStore(self.clients.bigquery, table)

    def subscriber(self):
        return self.clients.subscriber

    def config_values(self) -> dict:
        return {"landing_bucket": self.gcp["landing_bucket"]}

    def repo_root(self):
        return str(REPO_ROOT)

    # --- events -------------------------------------------------------------------------------------
    def parse_event(self, payload: dict, attributes: dict) -> dict:
        return event_mod.normalize(payload, attributes)

    def event_from_reference(self, reference: str) -> dict:
        tools = {t.name: t for t in self._audit_tools}
        _, job = tools["get_job"].func(job_id=reference)
        if not job:
            raise ValueError(f"no job_metadata row for {reference}")
        _, checks = tools["get_validation_results"].func(job_id=reference)
        failed = [{"check_name": c["check_name"], "check_type": c["check_type"], "summary": c["summary"],
                   "failed_rules": ",".join(c.get("failed_checks") or []), "details_json": c.get("details")}
                  for c in checks if c["validation_result"] == "FAIL"]  # fmt: skip
        payload = event_mod.payload_from_audit(job, failed, self.gcp["project"], self.gcp["region"], self.gcp["workflow"])
        return self.parse_event(payload, {})

    def incident_check(self, event: dict) -> tuple:
        job, status = event["job"], event["status"]
        if status == "SUCCESS":
            return False, (f"Job completed successfully - nothing failed: {job.get('rows_loaded')} of "
                           f"{job.get('rows_in_file')} rows loaded into {job.get('target_table')}, validation "
                           f"{job.get('validation_status')}, reconciliation {job.get('recon_status')}. "
                           f"File archived at {event['file'].get('final_path')}.")  # fmt: skip
        if status == "IN_PROGRESS":
            return False, "Job is still in progress (not failed). Re-check after it finishes."
        if status == "DUPLICATE_SKIPPED":
            return False, "Duplicate delivery of an already-loaded file generation; skipped as designed."
        return True, ""

    def severity(self, event: dict) -> str:
        level = 2 if event["status"] == "FAILED" else 3
        try:
            job_name = event["job"].get("job_name")
            tool = next(t for t in self._audit_tools if t.name == "get_recent_failures")
            _, rows = tool.func(job_name=job_name, hours=24)
            failures = sum(r["jobs"] for r in rows if r["status"] in ("FAILED", "REJECTED"))
            if failures >= 3:
                level -= 1
        except Exception:
            pass
        return f"SEV{level}"

    # --- agents -------------------------------------------------------------------------------------
    def gatherers(self) -> list:
        return [
            Gatherer("audit_agent", "audit",
                     "You investigate the pipeline's audit tables (job_metadata, validation_metadata, agent_incidents).",
                     self._audit_tools, default_audit_calls),
            Gatherer("gcp_agent", "gcp",
                     "You investigate Google Cloud: Dataflow job state/errors, Cloud Logging, the workflow execution, "
                     "GCS objects and regional quotas.",
                     self._gcp_tools, default_gcp_calls),
        ]  # fmt: skip

    # --- actions ------------------------------------------------------------------------------------
    def build_actions(self, category: str, event: dict, playbook: list) -> list:
        bucket = self.gcp["landing_bucket"]
        actions = []
        for entry in playbook:
            action = {"type": entry["type"], "params": {}, "description": " ".join(entry["default_description"].split()),
                      "steps": []}  # fmt: skip
            if entry["type"] == "RETRY_FILE":
                source = _object_name(event["file"].get("final_path"), bucket)
                destination = event["file"].get("name")
                action["params"] = {"bucket": bucket, "source": source, "destination": destination}
                action["manual_command"] = (f"gcloud storage cp gs://{bucket}/{source} gs://{bucket}/{destination} "
                                            f"--project {self.gcp['project']}")  # fmt: skip
            actions.append(action)
        return actions

    def policy_facts(self, action: dict, event: dict, root_cause: dict) -> dict:
        facts = {"event_status": event["status"], "category": root_cause.get("category"),
                 "confidence": root_cause.get("confidence")}  # fmt: skip
        if action["type"] == "RETRY_FILE":
            params = action["params"]
            bucket = self.clients.storage.bucket(self.gcp["landing_bucket"])
            facts["source_exists"] = bool(params.get("source")) and bucket.get_blob(params["source"]) is not None
            facts["destination_exists"] = bool(params.get("destination")) and bucket.get_blob(params["destination"]) is not None
            facts["retries_24h"] = self.store.retries_24h(event["subject"]) if self.store else 0
            tool = next(t for t in self._gcp_tools if t.name == "list_active_ingest_jobs")
            facts["stuck_ingest_jobs"] = tool.func()[1]["stuck"]
        return facts

    def execute(self, action: dict, incident_id: str) -> dict:
        if action["type"] == "RETRY_FILE":
            return retry_file(self.clients, self.gcp["landing_bucket"], action, incident_id)
        raise NotImplementedError(f"{action['type']} is not executable")

    # --- verification + remediation ------------------------------------------------------------------
    def _worker_sa(self, evidence: list) -> str:
        """The service account the Dataflow job ran as (from get_dataflow_job), else the configured one."""
        for e in evidence:
            if e["kind"] == "get_dataflow_job" and isinstance(e.get("data"), dict) and e["data"].get("service_account"):
                return e["data"]["service_account"]
        return self.gcp.get("dataflow_sa") or ""

    @staticmethod
    def _dataset_of(resource: str | None) -> str:
        """'dataset bigquerytarun:raw' / 'bigquerytarun:raw.customer' / 'bigquerytarun.raw.customer' -> 'raw'."""
        if not resource:
            return ""
        ref = resource.split()[-1].replace(":", ".")
        parts = ref.split(".")
        return parts[1] if len(parts) >= 2 else ""

    def verification_calls(self, signature: dict, digest: dict, event: dict, evidence: list) -> list:
        primary = digest.get("primary") or {}
        if signature["id"] == "permission_denied" and primary.get("permission"):
            member = self._worker_sa(evidence)
            if member:
                return [("check_permission", {"member": member, "permission": primary["permission"],
                                              "dataset": self._dataset_of(primary.get("resource"))})]  # fmt: skip
        if signature["id"] == "template_missing":
            return [("get_gcs_object", {"path": f"gs://{self.gcp['dataflow_bucket']}/templates/gcs_to_bq.json"})]
        return []

    _SUGGESTED_ROLES = {
        "bigquery.tables.create": "roles/bigquery.dataEditor", "bigquery.tables.updateData": "roles/bigquery.dataEditor",
        "bigquery.tables.getData": "roles/bigquery.dataViewer", "bigquery.tables.get": "roles/bigquery.dataViewer",
        "bigquery.jobs.create": "roles/bigquery.jobUser", "storage.objects.create": "roles/storage.objectAdmin",
        "storage.objects.get": "roles/storage.objectViewer", "storage.objects.delete": "roles/storage.objectAdmin",
        "dataflow.jobs.create": "roles/dataflow.developer", "iam.serviceAccounts.actAs": "roles/iam.serviceAccountUser",
    }  # fmt: skip

    def remediation_context(self, state: dict) -> dict:
        rc, event = state.get("root_cause") or {}, state["event"]
        primary = rc.get("primary_error") or {}
        verification = rc.get("verification") or {}
        principal = self._worker_sa(state.get("evidence", []))
        permission = primary.get("permission") or ""
        return {
            "incident_id": state["incident_id"],
            "project": self.gcp["project"],
            "principal": principal or "the Dataflow worker service account",
            "permission": permission or "(see error)",
            "resource": primary.get("resource") or "(see error)",
            "suggested_role": self._SUGGESTED_ROLES.get(permission, f"a role that includes {permission or 'the permission'}"),
            "error_message": (primary.get("message") or event["job"].get("error_message") or "")[:300],
            "step": rc.get("failing_step") or "",
            "verification": (f"Verified against live IAM: {verification['summary']}." if verification.get("verdict") == "CONFIRMED"
                             else ""),  # fmt: skip
        }

    # --- report -------------------------------------------------------------------------------------
    def report_sections(self, state: dict) -> dict:
        event = state["event"]
        job, file, dataflow = event["job"], event["file"], event["dataflow"]
        project, dataset = self.gcp["project"], self.gcp["audit_dataset"]
        facts = [
            ("Job ID", event["incident_id"]), ("Status", event["status"]), ("Job name", job.get("job_name")),
            ("Contract", job.get("contract_name")), ("Target table", job.get("target_table")),
            ("File", file.get("original_path")), ("Generation", file.get("generation")),
            ("File now at", file.get("final_path")), ("Rows in file / loaded", f"{job.get('rows_in_file')} / {job.get('rows_loaded')}"),
            ("Validation / recon", f"{job.get('validation_status')} / {job.get('recon_status')}"),
            ("Error code", job.get("error_code")), ("Error message", (job.get("error_message") or "")[:300]),
            ("Dataflow job", f"{dataflow.get('job_id') or '-'} ({dataflow.get('state') or '-'})"),
            ("Workflow execution", event["orchestration"].get("execution_id")),
        ]  # fmt: skip
        timeline = [(t, what) for t, what in [
            (job.get("start_time"), "job registered by the workflow"),
            *[(m.get("time"), f"Dataflow error: {m.get('text', '')[:150]}") for m in dataflow.get("error_messages") or []],
            (job.get("end_time"), f"job finished: {job.get('status')}"),
        ] if t]  # fmt: skip
        links = {k: v for k, v in event["links"].items() if v}
        links["job_metadata SQL"] = f"SELECT * FROM `{project}.{dataset}.job_metadata` WHERE job_id = '{event['incident_id']}'"
        links["validation_metadata SQL"] = (f"SELECT * FROM `{project}.{dataset}.validation_metadata` "
                                            f"WHERE job_id = '{event['incident_id']}'")  # fmt: skip
        return {"facts": facts, "timeline": sorted(timeline, key=lambda x: str(x[0])), "failed_checks": event["failed_checks"],
                "links": links}  # fmt: skip


def create_pack(cfg: dict) -> DataflowIngestPack:
    return DataflowIngestPack(cfg)
