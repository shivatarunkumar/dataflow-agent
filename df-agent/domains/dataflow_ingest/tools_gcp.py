"""GCP agent tools: typed read-only lookups via client libraries (no shell / arbitrary gcloud). Return (summary, data)."""

import json
import re
from datetime import datetime, timedelta, timezone

from incident_core.domain import ToolSpec
from incident_core.errors import clip

_ID = re.compile(r"^[A-Za-z0-9_\-.:]{1,200}$")  # ids interpolated into log filters / resource names


def _check_id(value: str, what: str) -> str:
    if not value or not _ID.match(value):
        raise ValueError(f"invalid {what}: {value!r}")
    return value


def _ts(value):
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _last_line(text: str) -> str:
    """The exception line of a traceback is its last non-empty line."""
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    return lines[-1][:300] if lines else ""


def _add_unique(items: list, item: dict, field: str):
    """Groups repeats (Dataflow retries a failing bundle 4x) by the tail of the text."""
    key = (item[field] or "")[-400:]
    for existing in items:
        if (existing[field] or "")[-400:] == key:
            existing["count"] = existing.get("count", 1) + 1
            return
    items.append({**item, "count": 1})


def _log_message(payload) -> str:
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        for key in ("message", "textPayload", "msg"):
            if isinstance(payload.get(key), str):
                return payload[key]
        return json.dumps(payload, default=str)
    return str(payload)


_ROLE_CACHE: dict = {}


def _role_permissions(clients, role: str) -> set:
    if role not in _ROLE_CACHE:
        try:
            _ROLE_CACHE[role] = set(clients.iam_admin.get_role(request={"name": role}).included_permissions)
        except Exception:
            _ROLE_CACHE[role] = set()  # unknown role: treated as not granting (reported as inconclusive upstream)
    return _ROLE_CACHE[role]


def build_gcp_tools(clients, gcp: dict) -> list:
    project, region = gcp["project"], gcp["region"]
    allowed_buckets = {b for b in (gcp.get("landing_bucket"), gcp.get("dataflow_bucket")) if b}

    def get_dataflow_job(dataflow_job_id: str):
        from google.cloud import dataflow_v1beta3

        job = clients.dataflow_jobs.get_job(request=dataflow_v1beta3.GetJobRequest(
            project_id=project, location=region, job_id=_check_id(dataflow_job_id, "dataflow job id"),
            view=dataflow_v1beta3.JobView.JOB_VIEW_ALL))  # fmt: skip
        environment = job.environment
        data = {"name": job.name, "state": job.current_state.name, "type": job.type_.name,
                "create_time": _ts(job.create_time), "state_time": _ts(job.current_state_time),
                "labels": dict(job.labels),
                "service_account": getattr(environment, "service_account_email", "") or ""}  # fmt: skip
        return (f"Dataflow job {dataflow_job_id}: {data['state']} (created {data['create_time']}, "
                f"runs as {data['service_account'] or 'unknown SA'})"), data  # fmt: skip

    def get_dataflow_errors(dataflow_job_id: str):
        """Full error texts (the exception is at the END of a traceback), de-duplicated across bundle retries."""
        from google.cloud import dataflow_v1beta3

        request = dataflow_v1beta3.ListJobMessagesRequest(
            project_id=project, location=region, job_id=_check_id(dataflow_job_id, "dataflow job id"),
            minimum_importance=dataflow_v1beta3.JobMessageImportance.JOB_MESSAGE_ERROR, page_size=50)  # fmt: skip
        unique, seen = [], 0
        for message in clients.dataflow_messages.list_job_messages(request=request):
            seen += 1
            _add_unique(unique, {"time": _ts(message.time), "text": clip(message.message_text, 12000)}, "text")
            if seen >= 50:
                break
        return (f"{seen} error messages ({len(unique)} distinct) for Dataflow job {dataflow_job_id}; "
                f"last line of first: {_last_line(unique[0]['text']) if unique else 'none'}"), unique  # fmt: skip

    presets = {
        "dataflow_errors": 'resource.type="dataflow_step" AND resource.labels.job_id="{id}" AND severity>=ERROR',
        "dataflow_job": 'resource.type="dataflow_step" AND resource.labels.job_id="{id}" AND severity>=WARNING',
        "dataflow_job_all": 'resource.type="dataflow_step" AND resource.labels.job_id="{id}"',
        "workflow_execution": 'resource.type="workflows.googleapis.com/Workflow" AND labels."workflows.googleapis.com/execution_id"="{id}"',
    }

    def get_logs(preset: str, resource_id: str, hours: int = 72, limit: int = 50):
        if preset not in presets:
            raise ValueError(f"preset must be one of {sorted(presets)}")
        since = (datetime.now(timezone.utc) - timedelta(hours=max(1, min(int(hours), 720)))).strftime("%Y-%m-%dT%H:%M:%SZ")
        log_filter = presets[preset].format(id=_check_id(resource_id, "resource id")) + f' AND timestamp>="{since}"'
        from google.cloud import logging as cloud_logging

        unique, seen = [], 0
        for entry in clients.logging.list_entries(filter_=log_filter, order_by=cloud_logging.DESCENDING,
                                                  max_results=max(1, min(int(limit), 200))):  # fmt: skip
            seen += 1
            _add_unique(unique, {"time": _ts(entry.timestamp), "severity": entry.severity,
                                 "message": clip(_log_message(entry.payload), 12000)}, "message")  # fmt: skip
        worst = [_last_line(e["message"]) for e in unique if e["severity"] in ("ERROR", "CRITICAL", "ALERT", "EMERGENCY")][:2]
        return f"{seen} log entries ({len(unique)} distinct, {preset} {resource_id}); errors end with: {worst or 'none'}", unique

    def get_workflow_execution(execution_id: str):
        name = (f"projects/{project}/locations/{region}/workflows/{gcp['workflow']}/executions/"
                f"{_check_id(execution_id, 'execution id')}")  # fmt: skip
        ex = clients.workflow_executions.get_execution(name=name)
        data = {"state": ex.state.name, "start_time": _ts(ex.start_time), "end_time": _ts(ex.end_time),
                "error": clip(ex.error.payload or "", 8000) if ex.error else "", "result": clip(ex.result or "", 4000)}  # fmt: skip
        return f"Workflow execution {execution_id}: {data['state']}; error: {data['error'][:200] or 'none'}", data

    def check_permission(member: str, permission: str, dataset: str = ""):
        """VERIFICATION: does `member` hold `permission` via its project roles or dataset roles?"""
        member = member if ":" in member else f"serviceAccount:{member}"
        email = member.split(":", 1)[1]
        policy = clients.resource_manager.get_iam_policy(resource=f"projects/{project}")
        project_roles = sorted(b.role for b in policy.bindings if member in b.members)
        dataset_roles = []
        if dataset:
            ds = clients.bigquery.get_dataset(f"{project}.{dataset}")
            legacy = {"OWNER": "roles/bigquery.dataOwner", "WRITER": "roles/bigquery.dataEditor", "READER": "roles/bigquery.dataViewer"}
            for entry in ds.access_entries:
                if entry.entity_id in (email, member):
                    dataset_roles.append(legacy.get(entry.role, entry.role))
        granted_by = [r for r in project_roles + dataset_roles if permission in _role_permissions(clients, r)]
        verdict = "REFUTED" if granted_by else "CONFIRMED"
        scope = f"dataset {dataset}" if dataset else "the project"
        summary = (f"{email} {'HAS' if granted_by else 'does NOT have'} {permission} "
                   f"(project roles: {project_roles or 'none'}; {scope} roles: {dataset_roles or 'none'}"
                   f"{'; granted by ' + ', '.join(granted_by) if granted_by else ''})")  # fmt: skip
        return summary, {"verdict": verdict, "member": member, "permission": permission, "dataset": dataset,
                         "project_roles": project_roles, "dataset_roles": dataset_roles, "granted_by": granted_by}  # fmt: skip

    def get_gcs_object(path: str):
        if not path.startswith("gs://"):
            raise ValueError("path must be gs://bucket/object")
        bucket_name, _, name = path[5:].partition("/")
        if bucket_name not in allowed_buckets:
            raise ValueError(f"bucket {bucket_name} is not one of the pipeline buckets")
        blob = clients.storage.bucket(bucket_name).get_blob(name)
        if blob is None:
            return f"{path} does not exist", {"exists": False}
        data = {"exists": True, "size": blob.size, "generation": blob.generation, "updated": _ts(blob.updated),
                "metadata": blob.metadata or {}}  # fmt: skip
        return f"{path} exists ({blob.size} bytes, generation {blob.generation})", data

    def get_quota(metric: str):
        region_info = clients.regions.get(project=project, region=region)
        quotas = [{"metric": q.metric, "limit": q.limit, "usage": q.usage} for q in region_info.quotas
                  if q.metric == metric.upper()]  # fmt: skip
        if not quotas:
            return f"no regional quota named {metric}", {}
        q = quotas[0]
        return f"quota {q['metric']} in {region}: usage {q['usage']} / limit {q['limit']}", q

    def list_active_ingest_jobs():
        from google.cloud import dataflow_v1beta3

        request = dataflow_v1beta3.ListJobsRequest(project_id=project, location=region,
                                                   filter=dataflow_v1beta3.ListJobsRequest.Filter.ACTIVE)  # fmt: skip
        now = datetime.now(timezone.utc)
        jobs = []
        for job in clients.dataflow_jobs.list_jobs(request=request):
            if dict(job.labels).get("pipeline") != "gcs-to-bq":
                continue
            age_min = (now - job.create_time).total_seconds() / 60 if job.create_time else None
            jobs.append({"id": job.id, "name": job.name, "state": job.current_state.name, "age_minutes": age_min})
        stuck = [j for j in jobs if (j["age_minutes"] or 0) > 60]
        return f"{len(jobs)} active ingestion jobs; {len(stuck)} running > 60 min", {"jobs": jobs, "stuck": len(stuck)}

    s = lambda d: {"type": "string", "description": d}  # noqa: E731
    return [
        # signal=True: this job's own errors / logs / orchestration error.
        ToolSpec("get_dataflow_errors", "Error messages of a Dataflow job.", get_dataflow_errors,
                 {"dataflow_job_id": s("Dataflow job id")}, signal=True),
        ToolSpec("get_logs", f"Cloud Logging entries. preset one of {sorted(presets)}.", get_logs,
                 {"preset": s("log preset"), "resource_id": s("Dataflow job id or workflow execution id"),
                  "hours": {"type": "integer", "description": "look-back hours", "required": False}}, signal=True),
        ToolSpec("get_workflow_execution", "State and error payload of the orchestration workflow execution.",
                 get_workflow_execution, {"execution_id": s("workflow execution id")}, signal=True),
        # Context only: state/readings, never treated as an error by the signature rules.
        ToolSpec("get_dataflow_job", "State and timing of a Dataflow job.", get_dataflow_job,
                 {"dataflow_job_id": s("Dataflow job id")}),
        ToolSpec("get_gcs_object", "Existence/size/metadata of a pipeline GCS object.", get_gcs_object,
                 {"path": s("gs://bucket/object")}),
        ToolSpec("get_quota", "Regional Compute quota usage, e.g. IN_USE_ADDRESSES or CPUS.", get_quota,
                 {"metric": s("quota metric name")}),
        ToolSpec("list_active_ingest_jobs", "Currently active ingestion Dataflow jobs.", list_active_ingest_jobs, {}),
        # Verification (context): confirms/refutes a missing-permission hypothesis against live IAM.
        ToolSpec("check_permission", "Whether a principal holds an IAM permission via its project/dataset roles.",
                 check_permission, {"member": s("principal email or serviceAccount:email"),
                                    "permission": s("e.g. bigquery.tables.create"),
                                    "dataset": {"type": "string", "description": "BigQuery dataset id", "required": False}}),
    ]  # fmt: skip


def default_gcp_calls(event: dict) -> list:
    dataflow, calls = event["dataflow"], []
    if dataflow.get("job_id"):
        calls += [("get_dataflow_job", {"dataflow_job_id": dataflow["job_id"]}),
                  ("get_dataflow_errors", {"dataflow_job_id": dataflow["job_id"]}),
                  ("get_logs", {"preset": "dataflow_errors", "resource_id": dataflow["job_id"]})]  # fmt: skip
    execution_id = event["orchestration"].get("execution_id")
    if execution_id:
        calls.append(("get_workflow_execution", {"execution_id": execution_id}))
    if event["file"].get("final_path"):
        calls.append(("get_gcs_object", {"path": event["file"]["final_path"]}))
    if event["status"] == "FAILED":
        calls += [("get_quota", {"metric": "IN_USE_ADDRESSES"}), ("get_quota", {"metric": "CPUS"}),
                  ("list_active_ingest_jobs", {})]  # fmt: skip
    return calls
