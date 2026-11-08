"""GCP agent tools: typed read-only lookups via client libraries (no shell / arbitrary gcloud). Return (summary, data)."""

import json
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from incident_core.domain import ToolSpec
from incident_core.errors import clip

_ID = re.compile(r"^[A-Za-z0-9_\-.:]{1,200}$")  # ids interpolated into log filters / resource names
_RUN_ID = re.compile(r"^[A-Za-z0-9_.~:+\-]{1,250}$")  # Airflow's allowed run_id pattern

INGEST_DAG_ID = "gcs_to_bq_dataflow"
DATAFLOW_TASK_ID = "run_dataflow"


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

    # --- Airflow (Composer) REST API: read-only GETs -------------------------------------------------------
    def _run_path(dag_run_id: str) -> str:
        if not dag_run_id or not _RUN_ID.match(dag_run_id):
            raise ValueError(f"invalid dag_run_id: {dag_run_id!r}")
        return f"dags/{INGEST_DAG_ID}/dagRuns/{quote(dag_run_id, safe='')}"

    def _task_instances(dag_run_id: str) -> list:
        body = clients.airflow.get(f"{_run_path(dag_run_id)}/taskInstances")
        keep = ("task_id", "state", "try_number", "map_index", "operator", "queued_when", "start_date", "end_date",
                "duration", "hostname")  # fmt: skip
        return [{k: ti.get(k) for k in keep} for ti in body.get("task_instances", [])]

    def get_airflow_dag_run(dag_run_id: str):
        run = clients.airflow.get(_run_path(dag_run_id))
        data = {k: run.get(k) for k in ("dag_run_id", "state", "run_type", "start_date", "end_date", "logical_date",
                                        "external_trigger", "conf", "note")}  # fmt: skip
        return (f"Airflow {INGEST_DAG_ID} run {dag_run_id}: {data['state']} "
                f"(started {data['start_date']}, ended {data['end_date'] or '-'})"), data  # fmt: skip

    def get_airflow_task_instances(dag_run_id: str):
        tasks = _task_instances(dag_run_id)
        failed = [t["task_id"] for t in tasks if t["state"] in ("failed", "upstream_failed")]
        states = ", ".join(f"{t['task_id']}={t['state']}" for t in tasks)
        return f"{len(tasks)} task instances: {states}; failed: {failed or 'none'}", {"tasks": tasks, "failed": failed}

    def get_airflow_task_log(dag_run_id: str, task_id: str, try_number: int = 0):
        """The task's log for one try (latest if try_number is 0). The exception is at the end, so head+tail is kept."""
        _check_id(task_id, "task id")
        try_number = int(try_number or 0)
        if try_number <= 0:
            task = next((t for t in _task_instances(dag_run_id) if t["task_id"] == task_id), None)
            if task is None:
                return f"task {task_id} is not part of run {dag_run_id}", {"task_id": task_id, "text": ""}
            try_number = max(int(task.get("try_number") or 1), 1)
        text = clients.airflow.get(f"{_run_path(dag_run_id)}/taskInstances/{task_id}/logs/{try_number}",
                                   params={"full_content": "true"}, text=True)  # fmt: skip
        return (f"Airflow log {task_id} try {try_number} ({len(text)} chars); ends with: {_last_line(text) or 'empty'}",
                {"task_id": task_id, "try_number": try_number, "text": clip(text, 12000, head=1500)})  # fmt: skip

    def find_dataflow_job(audit_job_id: str):
        """The Dataflow job the DAG launched for this audit job (deterministic name ingest-<job_id>), if any."""
        from google.cloud import dataflow_v1beta3

        name = f"ingest-{_check_id(audit_job_id, 'job id')}"
        request = dataflow_v1beta3.ListJobsRequest(project_id=project, location=region, page_size=100,
                                                   filter=dataflow_v1beta3.ListJobsRequest.Filter.ALL)  # fmt: skip
        for i, job in enumerate(clients.dataflow_jobs.list_jobs(request=request)):
            if job.name == name:
                data = {"exists": True, "id": job.id, "name": name, "state": job.current_state.name,
                        "create_time": _ts(job.create_time)}  # fmt: skip
                return f"Dataflow job {name} = {job.id}: {data['state']}", data
            if i >= 300:
                break
        return f"no Dataflow job named {name} exists: the launch never created one", {"exists": False, "name": name}

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
                 {"preset": s("log preset"), "resource_id": s("Dataflow job id"),
                  "hours": {"type": "integer", "description": "look-back hours", "required": False}}, signal=True),
        ToolSpec("get_airflow_task_log", "Log of one Airflow task try of the ingestion DAG run (the exception is at the end).",
                 get_airflow_task_log, {"dag_run_id": s("Airflow dag_run_id"), "task_id": s("task id, e.g. run_dataflow"),
                                        "try_number": {"type": "integer", "description": "try (0 = latest)", "required": False}},
                 signal=True),
        # Context only: state/readings, never treated as an error by the signature rules.
        ToolSpec("get_airflow_dag_run", "State, timing and conf of the Airflow ingestion DAG run.", get_airflow_dag_run,
                 {"dag_run_id": s("Airflow dag_run_id")}),
        ToolSpec("get_airflow_task_instances", "State, try number and duration of every task in the DAG run.",
                 get_airflow_task_instances, {"dag_run_id": s("Airflow dag_run_id")}),
        ToolSpec("find_dataflow_job", "Find the Dataflow job launched for an audit job_id (or confirm none exists).",
                 find_dataflow_job, {"audit_job_id": s("job id (UUID)")}),
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


def airflow_failed_task(event: dict) -> str:
    """Task whose log explains the failure. Live events name it; replays don't, so a FAILED job reads run_dataflow."""
    orchestration = event["orchestration"]
    if "failed_task" in orchestration:
        return orchestration.get("failed_task") or ""
    return DATAFLOW_TASK_ID if event["status"] == "FAILED" else ""


def default_gcp_calls(event: dict) -> list:
    """Top-down: Airflow DAG run -> failed task + its log -> Dataflow job -> Dataflow errors/logs -> platform context."""
    dataflow, orchestration, calls = event["dataflow"], event["orchestration"], []
    if orchestration.get("type") == "airflow" and orchestration.get("dag_run_id"):
        run_id = orchestration["dag_run_id"]
        calls += [("get_airflow_dag_run", {"dag_run_id": run_id}), ("get_airflow_task_instances", {"dag_run_id": run_id})]
        failed_task = airflow_failed_task(event)
        if failed_task:
            calls.append(("get_airflow_task_log", {"dag_run_id": run_id, "task_id": failed_task}))
        if not dataflow.get("job_id") and event["status"] == "FAILED" and event["job"].get("job_id"):
            calls.append(("find_dataflow_job", {"audit_job_id": event["job"]["job_id"]}))
    if dataflow.get("job_id"):
        calls += [("get_dataflow_job", {"dataflow_job_id": dataflow["job_id"]}),
                  ("get_dataflow_errors", {"dataflow_job_id": dataflow["job_id"]}),
                  ("get_logs", {"preset": "dataflow_errors", "resource_id": dataflow["job_id"]})]  # fmt: skip
    if event["file"].get("final_path"):
        calls.append(("get_gcs_object", {"path": event["file"]["final_path"]}))
    if event["status"] == "FAILED":
        calls += [("get_quota", {"metric": "IN_USE_ADDRESSES"}), ("get_quota", {"metric": "CPUS"}),
                  ("list_active_ingest_jobs", {})]  # fmt: skip
    return calls
