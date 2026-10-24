"""In-memory DomainPack built on the real dataflow_ingest event/actions logic, with fake tools and executor."""

from domains.dataflow_ingest.event import normalize
from domains.dataflow_ingest.pack import DataflowIngestPack
from incident_core.domain import Gatherer, ToolSpec
from incident_core.store import CompositeStore, LocalIncidentStore

QUOTA_ERROR = ("Failed to start the launcher VM, launcher-2026101809535717354824198720807966, because of status code: "
               "RESOURCE_EXHAUSTED, reason: [QUOTA_EXCEEDED] 'Quota 'IN_USE_ADDRESSES' exceeded.  Limit: 4.0 in region europe-west2.'.")  # fmt: skip


def failed_quota_payload(job_id="job-quota-1"):
    return {
        "schema_version": "1",
        "job": {"job_id": job_id, "status": "FAILED", "error_code": "DATAFLOW_JOB_FAILED", "job_name": "customer_load",
                "error_message": "Dataflow job 2026-10-18_df ended in JOB_STATE_FAILED. "},  # fmt: skip
        "file": {"bucket": "test-landing", "name": "landing/customer_20261015.csv", "generation": "1",
                 "original_path": "gs://test-landing/landing/customer_20261015.csv",
                 "final_path": "gs://test-landing/invalid/2026/10/18/customer_20261015.csv"},  # fmt: skip
        "failed_checks": [],
        "dataflow": {"job_id": "2026-10-18_df", "state": "JOB_STATE_FAILED", "launch_error": "",
                     "error_messages": [{"time": "2026-10-18T09:53:57Z", "text": QUOTA_ERROR}]},  # fmt: skip
        "orchestration": {"workflow": "dfa-ingest-file", "execution_id": "exec-1"},
    }


def rejected_header_payload(job_id="job-header-1"):
    return {
        "schema_version": "1",
        "job": {"job_id": job_id, "status": "REJECTED", "error_code": "HEADER", "job_name": "user_load"},
        "file": {"bucket": "test-landing", "name": "landing/user_20261016.csv", "generation": "7",
                 "final_path": "gs://test-landing/invalid/2026/10/18/user_20261016.csv"},  # fmt: skip
        "failed_checks": [{"check_name": "header", "check_type": "FILE_FORMAT",
                           "summary": "Header mismatch: missing=['role'], unexpected=[]", "failed_rules": "HEADER_MATCH"}],  # fmt: skip
        "dataflow": {"job_id": "2026-10-18_hdr", "state": "JOB_STATE_DONE", "error_messages": []},
        "orchestration": {"workflow": "dfa-ingest-file", "execution_id": "exec-2"},
    }


class FakePack(DataflowIngestPack):
    """Real event parsing, action building and reporting; fake evidence, facts and execution."""

    WORKER_SA = "dfa-dataflow@bigquerytarun.iam.gserviceaccount.com"

    def __init__(self, cfg, facts=None, permission_verdict="CONFIRMED"):
        self.cfg, self.gcp, self.store, self.clients = cfg, cfg["gcp"], None, None
        self.facts_override = facts or {}
        self.permission_verdict = permission_verdict
        self.executed = []
        self.verified = []

    def gatherers(self):
        def job(job_id):
            return f"job {job_id} found", {"job_id": job_id}

        def dataflow_job(dataflow_job_id):  # context: names the worker SA
            return f"Dataflow job {dataflow_job_id}: JOB_STATE_FAILED", {"state": "JOB_STATE_FAILED", "service_account": self.WORKER_SA}

        def quota(metric):  # context reading that names the quota metric but is NOT an error
            return f"quota {metric} in europe-west2: usage 0 / limit 4", {"metric": metric, "usage": 0, "limit": 4}

        def check_permission(member, permission, dataset=""):  # fake live IAM check
            self.verified.append((member, permission, dataset))
            has = self.permission_verdict == "REFUTED"
            return (f"{member} {'HAS' if has else 'does NOT have'} {permission} (project roles: ['roles/dataflow.worker'])",
                    {"verdict": self.permission_verdict, "member": member, "permission": permission})  # fmt: skip

        audit = Gatherer("audit_agent", "audit", "fake audit", [ToolSpec("get_job", "", job, {"job_id": {}}, signal=True)],
                         lambda e: [("get_job", {"job_id": e["incident_id"]})])  # fmt: skip
        gcp = Gatherer("gcp_agent", "gcp", "fake gcp",
                       [ToolSpec("get_dataflow_errors", "", self.dataflow_errors_tool(), {"dataflow_job_id": {}}, signal=True),
                        ToolSpec("get_dataflow_job", "", dataflow_job, {"dataflow_job_id": {}}),
                        ToolSpec("get_quota", "", quota, {"metric": {}}),
                        ToolSpec("check_permission", "", check_permission, {"member": {}, "permission": {}, "dataset": {}})],
                       lambda e: [("get_dataflow_job", {"dataflow_job_id": e["dataflow"]["job_id"]}),
                                  ("get_dataflow_errors", {"dataflow_job_id": e["dataflow"]["job_id"]}),
                                  ("get_quota", {"metric": "IN_USE_ADDRESSES"})] if e["status"] == "FAILED" else [])  # fmt: skip
        return [audit, gcp]

    def dataflow_errors_tool(self):
        def errors(dataflow_job_id):
            texts = getattr(self, "error_texts", None) or [getattr(self, "error_text", QUOTA_ERROR)]
            return f"{len(texts)} error(s): {texts[0][:80]}", [{"time": "2026-10-20T13:21:52Z", "text": t} for t in texts]

        return errors

    def severity(self, event):
        return "SEV2" if event["status"] == "FAILED" else "SEV3"

    def policy_facts(self, action, event, root_cause):
        facts = {"event_status": event["status"], "category": root_cause["category"], "confidence": root_cause["confidence"],
                 "source_exists": True, "destination_exists": False, "retries_24h": 0, "stuck_ingest_jobs": 0}  # fmt: skip
        facts.update(self.facts_override)
        return facts

    def execute(self, action, incident_id):
        self.executed.append(action)
        return {"gcs_uri": f"gs://{action['params']['bucket']}/{action['params']['destination']}"}

    def event(self, payload):
        return normalize(payload, {})


# The real worker error from incident ba0fbdb5 (SA lost roles/bigquery.dataEditor), as Dataflow reports it.
PERMISSION_TRACEBACK = """Error message from worker: Traceback (most recent call last):
  File "apache_beam/runners/common.py", line 1512, in apache_beam.runners.common.DoFnRunner.process
  File "apache_beam/runners/common.py", line 914, in apache_beam.runners.common.PerWindowInvoker.invoke_process
  File "/usr/local/lib/python3.12/site-packages/apache_beam/io/gcp/bigquery_file_loads.py", line 760, in process
    job_reference = self.bq_wrapper.perform_load_job(
  File "/usr/local/lib/python3.12/site-packages/apache_beam/io/gcp/bigquery_tools.py", line 1080, in perform_load_job
    return self._insert_load_job(
RuntimeError: apitools.base.py.exceptions.HttpForbiddenError: HttpError accessing <https://bigquery.googleapis.com/bigquery/v2/projects/bigquerytarun/jobs?alt=json>: response: <{'vary': 'Origin, X-Origin, Referer', 'content-type': 'application/json; charset=UTF-8', 'status': '403', 'content-length': '482', '-content-encoding': 'gzip'}>, content <{
  "error": {
    "code": 403,
    "message": "Access Denied: Dataset bigquerytarun:raw: Permission bigquery.tables.create denied on dataset bigquerytarun:raw (or it may not exist).",
    "errors": [
      {
        "message": "Access Denied: Dataset bigquerytarun:raw: Permission bigquery.tables.create denied on dataset bigquerytarun:raw (or it may not exist).",
        "domain": "global",
        "reason": "accessDenied"
      }
    ],
    "status": "PERMISSION_DENIED"
  }
}
>"""

WORKFLOW_FAILED = ("Workflow failed. Causes: S56:LoadToTargetTable/WriteToBigQuery/BigQueryBatchFileLoads/"
                   "ReshuffleBeforeLoadWithTempTables/ReshufflePerKey/GroupByKey/Read+LoadToTargetTable/WriteToBigQuery "
                   "failed., A work item was attempted 4 times without success.")  # fmt: skip


def local_store(cfg):
    return CompositeStore(None, LocalIncidentStore(cfg["state_dir"]))
