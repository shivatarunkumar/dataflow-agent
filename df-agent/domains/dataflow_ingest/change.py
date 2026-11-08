"""ServiceNow Change Request draft for an ingestion incident (df-agent.md §21). Deterministic: every value is derived
from the incident state (event, root cause, plan, decisions, evidence); nothing is invented by an LLM.

Rendering lives in incident_core/report/change.py; this module decides whether a CR is needed and fills it.
"""

from incident_core.errors import describe
from incident_core.models import NEEDS_HUMAN

CHANGE_ACTIONS = ("PROPOSE_CODE_CHANGE", "PROPOSE_CONFIG_CHANGE")

# ServiceNow choice values (change_request.risk / impact / urgency / priority).
RISK_CODES = {"High": "2", "Moderate": "3", "Low": "4"}
LEVELS = {1: "1 - High", 2: "2 - Medium", 3: "3 - Low"}
PRIORITY = {(1, 1): 1, (1, 2): 2, (2, 1): 2, (1, 3): 3, (2, 2): 3, (3, 1): 3, (2, 3): 4, (3, 2): 4, (3, 3): 5}
PRIORITY_NAMES = {1: "1 - Critical", 2: "2 - High", 3: "3 - Moderate", 4: "4 - Low", 5: "5 - Planning"}

# Estimated hands-on duration per change, minutes (deploy + re-run + verification; a Dataflow run is ~6-8 min).
DURATION = {"dataflow_code": 60, "dag_code": 30, "config": 30, "retry": 15}

CONFIG_COMPONENTS = {
    "permission_denied": "IAM bindings of {principal}",
    "template_missing": "Dataflow Flex Template gcs-to-bq (templates/gcs_to_bq.json + image)",
    "dataflow_launch_rejected": "Dataflow Flex Template launch parameters (dags/gcs_to_bq_dataflow.py, dataflow-job/metadata.json)",
    "quota_exceeded": "Compute Engine regional quota / max_concurrent_jobs",
    "bq_rate_limit": "BigQuery quota / max_concurrent_jobs",
    "bq_not_found": "BigQuery datasets and tables (infra/bigquery.tf)",
}


# Config fixes that are not made in Terraform (rebuild the template / fix the DAG launch body).
NON_TERRAFORM_SIGNATURES = {"template_missing", "dataflow_launch_rejected"}


def _severity_level(severity: str) -> int:
    digits = "".join(ch for ch in str(severity or "") if ch.isdigit())
    return min(max(int(digits), 1), 3) if digits else 3


def _decided(state: dict) -> list:
    return [(a, d) for a, d in zip(state.get("plan") or [], state.get("decisions") or [])]


def _no_change(reason: str, instead: list) -> dict:
    return {"required": False, "reason": reason, "instead": instead}


def _not_required(state: dict) -> dict | None:
    """The reason no CR is needed, or None if one is."""
    event, rc = state["event"], state.get("root_cause") or {}
    decided = _decided(state)
    types = {a["type"] for a, _ in decided}
    pending_retry = any(a["type"] == "RETRY_FILE" and d["decision"] == NEEDS_HUMAN for a, d in decided)
    if types & set(CHANGE_ACTIONS) or pending_retry:
        return None
    if state.get("no_incident_reason"):
        return _no_change(f"Not an incident: {state['no_incident_reason']}", [])
    executed = [e for e in state.get("executed") or [] if e.get("action_type") == "RETRY_FILE" and e.get("status") == "DONE"]
    if executed:
        return _no_change(
            "The file was re-run automatically under the agent's retry policy (transient platform failure). Nothing in "
            "the service changes, so no change record is needed; the retry is recorded in the report and in "
            "file_audit.agent_incidents.",
            ["Check that the re-run succeeded (the new job for the same file in file_audit.job_metadata).",
             "If the same transient error repeats, a capacity/config change may be needed: re-investigate with "
             f"make agent-replay JOB_ID={state['incident_id']}."],
        )  # fmt: skip
    if "PROPOSE_DATA_FIX" in types:
        fix = next(a for a, _ in decided if a["type"] == "PROPOSE_DATA_FIX")
        return _no_change(
            f"The file {event.get('subject')} broke the data contract (status {event['status']}). The data owner corrects "
            "and re-uploads the file; nothing in the service changes.",
            [fix.get("description") or "Fix the file as described in the report's failed checks.", *fix.get("steps", []),
             f"Re-upload the corrected file to gs://<landing bucket>/landing/{event.get('subject')}."],
        )  # fmt: skip
    if "ESCALATE" in types:
        return _no_change(
            f"The root cause is not established yet (category {rc.get('category')}, confidence {rc.get('confidence')}). "
            "Raise a change request once the investigation identifies a change.",
            ["Review the evidence and open questions in report.pdf.",
             f"After adding information or fixing access, re-investigate: make agent-replay JOB_ID={state['incident_id']}."],
        )  # fmt: skip
    return _no_change("The plan contains no change to the service.", [])


def _short_cause(rc: dict) -> str:
    primary = rc.get("primary_error") or {}
    if primary.get("exception_chain") or primary.get("message"):
        chain = (primary.get("exception_chain") or [""])[-1]
        message = " ".join((primary.get("message") or "").split())[:70]
        return f"{chain}: {message}".strip(": ") if chain else message
    return " ".join((rc.get("summary") or rc.get("category") or "pipeline failure").split())[:90]


class ChangeDraft:
    """Builds the CR for one incident. `paths`/`layer` come from the domain pack (investigation_path, failing_layer)."""

    def __init__(self, cfg: dict, state: dict, path: list, layer: str, remediation: dict):
        self.cfg, self.state = cfg, state
        self.change_cfg = cfg.get("change") or {}
        self.gcp = cfg["gcp"]
        self.event, self.rc = state["event"], state.get("root_cause") or {}
        self.job, self.file = self.event["job"], self.event["file"]
        self.path, self.layer, self.remediation = path, layer, remediation
        self.decided = _decided(state)
        self.code = [a for a, _ in self.decided if a["type"] == "PROPOSE_CODE_CHANGE"]
        self.config = [a for a, _ in self.decided if a["type"] == "PROPOSE_CONFIG_CHANGE"]
        self.retry = next((a for a, d in self.decided if a["type"] == "RETRY_FILE" and d["decision"] == NEEDS_HUMAN), None)
        self.signature = (self.rc.get("signature") or {}).get("id", "")
        self.contract = self.job.get("contract_name") or (self.event.get("subject") or "").split("_", 1)[0] or "unknown"
        self.file_name = self.event.get("subject") or ""

    # --- classification ---------------------------------------------------------------------------------
    def kind(self) -> str:
        if self.code:
            return "dag_code" if self.layer == "AIRFLOW" else "dataflow_code"
        return "config" if self.config else "retry"

    def change_type(self) -> str:
        if not (self.code or self.config):
            return "Standard"
        return "Emergency" if _severity_level(self.state.get("severity")) == 1 else "Normal"

    def risk(self) -> tuple:
        reviews = (self.state.get("code_review") or {}).get("reviews", [])
        confidence = float(self.rc.get("confidence") or 0)
        if any(r.get("risk") == "HIGH" for r in reviews):
            return "High", "the code review rated the proposed change HIGH risk"
        if self.code and confidence < 0.7:
            return "High", f"a code change based on a root cause with only {confidence:.2f} confidence"
        if self.code:
            return "Moderate", "a code change to a component that processes every file of every contract"
        if self.config and self.signature == "permission_denied":
            return "Moderate", "an IAM change (least-privilege grant to a pipeline service account)"
        if self.config:
            return "Low", "a configuration change in Terraform, reviewed with terraform plan before apply"
        return "Low", "a create-only copy of one file; nothing in the service changes"

    def components(self) -> list:
        items = []
        if self.code:
            items.append("Airflow DAG gcs_to_bq_dataflow (dags/, Composer environment "
                         f"{self.gcp.get('composer_env') or 'dfa-composer'})" if self.kind() == "dag_code"
                         else "Dataflow pipeline image gcs-to-bq + Flex Template (dataflow-job/)")  # fmt: skip
        if self.config:
            template = CONFIG_COMPONENTS.get(self.signature, "Terraform-managed infrastructure (infra/)")
            items.append(template.format(principal=self.remediation.get("principal") or "the pipeline service account"))
        if self.retry:
            bucket = self.gcp.get("landing_bucket")
            items.append(f"Ingestion run of {self.file_name}" + (f" (gs://{bucket}/landing/)" if bucket else ""))
        return items

    def short_description(self) -> str:
        cause = _short_cause(self.rc)
        job_name = self.job.get("job_name") or f"{self.contract}_load"
        if self.code:
            where = "Airflow DAG" if self.kind() == "dag_code" else "Dataflow pipeline"
            text = f"Code fix in {where} for {job_name}: {cause}"
        elif self.config:
            text = f"Config change for {job_name}: {cause}"
        else:
            text = f"Re-run ingestion file {self.file_name} after {self.rc.get('category', 'a')} failure"
        return text[:160]

    # --- form fields --------------------------------------------------------------------------------------
    def fields(self) -> list:
        level = _severity_level(self.state.get("severity"))
        impact = urgency = level
        priority = PRIORITY[(impact, urgency)]
        risk, _ = self.risk()
        comps = self.components()
        return [
            {"label": "Type", "api": "type", "value": self.change_type(), "api_value": self.change_type().lower()},
            {"label": "Category", "api": "category", "value": self.change_cfg.get("category", "Software")},
            {"label": "Service", "api": "business_service", "value": self.change_cfg.get("service")},
            {"label": "Configuration item", "api": "cmdb_ci", "value": comps[0] if comps else ""},
            {"label": "Environment", "api": None, "value": self.change_cfg.get("environment")},
            {"label": "Risk", "api": "risk", "value": risk, "api_value": RISK_CODES[risk]},
            {"label": "Impact", "api": "impact", "value": LEVELS[impact], "api_value": str(impact)},
            {"label": "Urgency", "api": "urgency", "value": LEVELS[urgency], "api_value": str(urgency)},
            {"label": "Priority", "api": "priority", "value": PRIORITY_NAMES[priority], "api_value": str(priority)},
            {"label": "Assignment group", "api": "assignment_group", "value": self.change_cfg.get("assignment_group")},
            {"label": "Requested by", "api": "requested_by", "value": self.change_cfg.get("requested_by")},
            {"label": "State", "api": None, "value": "New"},
            {"label": "Estimated duration", "api": None, "value": f"{DURATION[self.kind()]} min"},
            {"label": "Related df-agent incident", "api": None, "value": f"{self.state['incident_id']} (severity {self.state.get('severity')})"},
            {"label": "Root cause", "api": None,
             "value": f"{self.rc.get('category')} (confidence {self.rc.get('confidence')}, via {self.rc.get('method') or '-'})"},
            {"label": "Failing layer", "api": None, "value": self.layer},
        ]  # fmt: skip

    def manual_fields(self) -> list:
        duration = DURATION[self.kind()]
        fields = [
            {"label": "Number", "api": "number", "hint": "Assigned by ServiceNow."},
            {"label": "Assigned to", "api": "assigned_to", "hint": "The engineer implementing the change."},
            {"label": "Planned start date", "api": "start_date",
             "hint": f"A window outside the file arrival times of the {self.contract} feed."},
            {"label": "Planned end date", "api": "end_date", "hint": f"Start + {duration} min (+ buffer for the backout)."},
            {"label": "Related incident (INC)", "api": "parent",
             "hint": f"The ServiceNow incident raised for this failure, if any (df-agent id {self.state['incident_id']})."},
        ]  # fmt: skip
        if self.change_type() != "Standard":
            fields.append({"label": "CAB date", "api": "cab_date", "hint": "Next CAB meeting before the planned start (Normal change)."
                           if self.change_type() == "Normal" else "Emergency CAB (ECAB) approval before implementation."})  # fmt: skip
        return fields

    # --- sections -----------------------------------------------------------------------------------------
    def description(self) -> dict:
        job, file = self.job, self.file
        paragraphs = [
            f"Ingestion job {self.state['incident_id']} for file {file.get('original_path') or self.file_name} ended "
            f"{self.event['status']}{' at ' + str(job.get('end_time')) if job.get('end_time') else ''}. "
            f"The failure happened in the {self.layer} layer.",
            f"Impact: the {self.contract} file was not loaded into {job.get('target_table') or 'its target table'} "
            f"({job.get('rows_in_file') or '?'} rows in the file, {job.get('rows_loaded') or 0} loaded). "
            f"The original file is kept at {file.get('final_path') or 'invalid/'}.",
            "Proposed change: " + " ".join(a.get("description", "") for a in self.code + self.config).strip()
            if self.code or self.config else "Proposed change: re-run the file now that the cause is resolved.",
            "Investigation path:",
        ]  # fmt: skip
        return {"title": "Description", "api": "description", "paragraphs": paragraphs,
                "bullets": [f"{layer}: {finding}" for layer, finding in self.path]}  # fmt: skip

    def justification(self) -> dict:
        rc = self.rc
        verification = rc.get("verification") or {}
        bullets = [f"Root cause: {' '.join((rc.get('summary') or '').split())}",
                   f"Category {rc.get('category')}, confidence {rc.get('confidence')}, determined by {rc.get('method') or '-'}"
                   + (f" (known failure signature {self.signature})" if self.signature else "") + "."]  # fmt: skip
        if rc.get("primary_error"):
            bullets.append(f"Primary error: {describe(rc['primary_error'])[:300]}")
        if verification.get("summary"):
            bullets.append(f"Verified against the live system: {verification.get('verdict')} - {verification['summary']}")
        if rc.get("evidence_refs"):
            bullets.append(f"Evidence: {', '.join(rc['evidence_refs'])} (details/evidence.json).")
        if self.code or self.config:
            bullets.append(f"Without this change every new {self.contract} file with the same characteristics fails the "
                           "same way, and the failed file cannot be loaded.")  # fmt: skip
        else:
            bullets.append("The file is still unloaded; the cause has been fixed, so a re-run is expected to succeed.")
        return {"title": "Justification", "api": "justification", "bullets": bullets}

    def _verify_sql(self) -> str:
        table = f"`{self.gcp['project']}.{self.gcp['audit_dataset']}.job_metadata`"
        return (f"SELECT job_id, status, error_code, rows_in_file, rows_loaded, validation_status, recon_status, created_at\n"
                f"FROM {table}\nWHERE file_name = '{self.file_name}'\nORDER BY created_at DESC LIMIT 3")  # fmt: skip

    def _retry_step(self) -> str:
        if self.retry and self.retry.get("manual_command"):
            return (f"Re-run the failed file (create-only copy, a new generation starts a new Airflow run): "
                    f"make agent-approve INCIDENT={self.state['incident_id']}  (or: {self.retry['manual_command']})")  # fmt: skip
        return f"Re-run the failed file: make agent-approve INCIDENT={self.state['incident_id']}"

    def implementation_plan(self) -> dict:
        kind = self.kind()
        steps = [
            "Pre-check: no newer successful job exists for this file (verification SQL below), so the change is still needed.",
            "Pre-check: make gcs-check passes and the Composer environment is RUNNING (make airflow-up if it is down).",
            "Pre-check: make airflow-runs shows no in-flight gcs_to_bq_dataflow run for this file.",
        ]  # fmt: skip
        terraform = kind == "config" and self.signature not in NON_TERRAFORM_SIGNATURES
        if terraform:
            steps += ["Make the change in Terraform (infra/), not in the console, so the next apply keeps it.",
                      "Review the plan before applying: make plan (only the intended resources change)."]  # fmt: skip
        for action in self.code + self.config:
            # The re-run is added once, at the end.
            steps += [s for s in action.get("steps") or [action.get("description", "")] if not s.lower().startswith("retry")]
        if kind == "dataflow_code":
            steps += ["Record the current image for the backout: gcloud artifacts docker images list "
                      f"{self.gcp['region']}-docker.pkg.dev/{self.gcp['project']}/dfa-dataflow --include-tags",
                      "Apply the fix in dataflow-job/ (proposed patch, if generated: details/proposed.patch) and merge it after review.",
                      "Run the unit tests: make test",
                      "Build and publish the image and the Flex Template spec: make docker"]  # fmt: skip
        elif kind == "dag_code":
            steps += ["Apply the fix in dags/ (proposed patch, if generated: details/proposed.patch) and merge it after review.",
                      "Run the DAG tests: make dags-test",
                      "Deploy the DAGs: make dags-deploy (Composer picks them up within ~1 minute)."]  # fmt: skip
        elif terraform and not any("gcs-setup" in step for step in steps):
            steps.append("Apply: make gcs-setup")
        steps += [self._retry_step(),
                  "Verify with the post-implementation checks below and close the incident."]  # fmt: skip
        return {"title": "Implementation plan", "api": "implementation_plan", "steps": list(dict.fromkeys(s for s in steps if s))}

    def risk_impact(self) -> dict:
        risk, why = self.risk()
        kind = self.kind()
        blast = {
            "dataflow_code": "every file of every contract processed after the deploy (customer, user, orders) uses the new image",
            "dag_code": "every file orchestrated by Airflow after the deploy (all contracts)",
            "config": f"the component(s) changed: {', '.join(self.components())}",
            "retry": f"only {self.file_name} and its target table {self.job.get('target_table') or ''}".strip(),
        }[kind]  # fmt: skip
        bullets = [
            f"Risk {risk}: {why}.",
            f"Blast radius: {blast}.",
            "Data impact: the change itself modifies no existing data. The re-run appends the file's rows to "
            f"{self.job.get('target_table') or 'the target table'} (append-only; a file generation is never loaded twice).",
            "Downtime: none. Files that land during the change queue on Pub/Sub (7 days) or wait in Airflow; a file that "
            "fails during the window is re-run afterwards from invalid/.",
            "Dependencies: the Composer environment must be running for the re-run; Dataflow jobs run one at a time "
            "(external-IP quota), so the re-run may queue behind other files.",
        ]  # fmt: skip
        for review in (self.state.get("code_review") or {}).get("reviews", []):
            bullets.append(f"Code review: {review.get('verdict')} (risk {review.get('risk')}): {'; '.join(review.get('comments') or [])}")
        confidence = float(self.rc.get("confidence") or 0)
        if confidence < 0.85:
            bullets.append(f"Root-cause confidence is {confidence:.2f}: confirm the hypothesis (e.g. reproduce with a "
                           "unit test or a manual run) before implementing.")  # fmt: skip
        return {"title": "Risk and impact analysis", "api": "risk_impact_analysis", "bullets": bullets}

    def test_plan(self) -> dict:
        kind = self.kind()
        steps = {
            "dataflow_code": [f"Add a unit test in dataflow-job/tests/ that reproduces the failure with a copy of {self.file_name}.",
                              "make test passes (new and existing tests).",
                              f"Optional smoke test before the deploy: make run-local FILE=<local copy of {self.file_name}>."],
            "dag_code": ["Add or extend a test in dags/tests/ that covers the failing path.", "make dags-test passes."],
            "config": ["make plan shows only the intended change (Terraform changes)." if self.signature not in NON_TERRAFORM_SIGNATURES
                       else "gcloud storage objects describe the template spec / make dags-test, depending on the fix.",
                       *[s for a in self.config for s in a.get("steps") or [] if s.lower().startswith("confirm")]],
            "retry": ["Confirm the cause is resolved (see Justification) before re-running."],
        }[kind]  # fmt: skip
        steps += [f"End to end: the re-run of {self.file_name} ends SUCCESS with validation PASS, recon PASS and "
                  "rows_loaded = rows_in_file (verification SQL below).",
                  "The Airflow run of the re-run is green, and df-agent raises no new incident for the file."]  # fmt: skip
        return {"title": "Test plan", "api": "test_plan", "steps": steps}

    def backout_plan(self) -> dict:
        kind = self.kind()
        target = self.job.get("target_table") or f"raw.{self.contract}"
        target = target if target.count(".") >= 2 else f"{self.gcp['project']}.{target}"  # job_metadata may hold project.dataset.table
        data_backout = (f"If the re-run loaded wrong data, the data owner removes exactly that load (approval required): "
                        f"DELETE FROM `{target}` WHERE _job_id = '<job_id of the re-run>'")  # fmt: skip
        steps = {
            "dataflow_code": ["git revert the fix and run make docker: the image and the Flex Template spec point back "
                              "to the previous code (or re-tag the image recorded in the implementation plan).",
                              "Files that failed during the change window: copy them from invalid/ back to landing/."],
            "dag_code": ["git revert the DAG change and run make dags-deploy.",
                         "Clear or re-run affected Airflow runs only after the revert is live."],
            "config": (["git revert the Terraform change, review make plan, then make gcs-setup."]
                       if self.signature not in NON_TERRAFORM_SIGNATURES else
                       ["git revert the change, then make template (spec) and/or make dags-deploy (DAG)."])
                      + ["Reverting restores the previous (failing) configuration; only back out if the change itself causes a problem."],
            "retry": ["Nothing to back out in the service: the re-run is a create-only copy of the file."],
        }[kind]  # fmt: skip
        return {"title": "Backout plan", "api": "backout_plan", "steps": steps + [data_backout]}

    def verification(self) -> dict:
        orchestration = self.event.get("orchestration") or {}
        return {"title": "Post-implementation verification", "api": None,
                "paragraphs": ["Run in BigQuery; the newest row must be SUCCESS:"], "code": self._verify_sql(),
                "bullets": [f"Airflow: the new gcs_to_bq_dataflow run for {self.file_name} is green "
                            f"(previous failed run: {orchestration.get('dag_run_id') or orchestration.get('execution_id') or '-'}).",
                            "Validation checks for the new job_id: SELECT check_name, validation_result FROM "
                            f"`{self.gcp['project']}.{self.gcp['audit_dataset']}.validation_metadata` WHERE job_id = '<new job_id>'"]}  # fmt: skip

    def communication(self) -> dict:
        owner = (self.change_cfg.get("data_owner") or "The data owner").format(contract=self.contract)
        channel = self.change_cfg.get("ops_channel") or "the operations channel"
        return {"title": "Communication plan", "api": None, "bullets": [
            f"Before: tell {owner} and {channel} the planned window, and that {self.contract} files may be delayed.",
            f"After: post the outcome (verification result, re-run job_id) in {channel} and to {owner}.",
            "Update the related incident with the CR number and close it after successful verification."]}  # fmt: skip

    def approvals(self) -> dict:
        bullets = {
            "Standard": ["Pre-approved standard change model \"Re-run ingestion file\" (create-only copy, no service change).",
                         "Approval recorded by running make agent-approve (logged in file_audit.agent_incidents)."],
            "Normal": ["Peer review of the change (pull request / terraform plan).", "Change manager approval.",
                       "CAB approval at the CAB date."],
            "Emergency": ["Emergency CAB (ECAB) approval before implementation (SEV1: repeated failures).",
                          "Retrospective review at the next CAB."],
        }[self.change_type()]  # fmt: skip
        return {"title": "Approvals", "api": None, "bullets": bullets}

    def related(self) -> dict:
        links = {k: v for k, v in (self.event.get("links") or {}).items() if v}
        bullets = [f"df-agent incident: {self.state['incident_id']} (report.pdf in the same folder)"]
        bullets += [f"{name}: {url}" for name, url in links.items()]
        return {"title": "Related records", "api": None, "bullets": bullets}

    def closure(self) -> dict:
        return {"title": "Closure", "api": None,
                "bullets": ["Close code: Successful | Successful with issues | Unsuccessful | Backed out",
                            "Close notes: what was changed, the re-run job_id and its result, any deviation from the plan."],
                "code": (f"Implemented {self.short_description()}. Re-run of {self.file_name}: job_id <id>, status <SUCCESS>, "
                         "rows <n>/<n>. Verification: <passed>. Deviations: <none>.")}  # fmt: skip

    def build(self) -> dict:
        return {
            "required": True,
            "short_description": self.short_description(),
            "fields": self.fields(),
            "manual_fields": self.manual_fields(),
            "sections": [self.description(), self.justification(), self.implementation_plan(), self.risk_impact(),
                         self.test_plan(), self.backout_plan(), self.verification(), self.communication(),
                         self.approvals(), self.related(), self.closure()],
        }  # fmt: skip


def build_change_request(cfg: dict, state: dict, path: list, layer: str, remediation: dict) -> dict:
    return _not_required(state) or ChangeDraft(cfg, state, path, layer, remediation).build()
