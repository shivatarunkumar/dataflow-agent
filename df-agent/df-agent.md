# df-agent — Incident-response multi-agent for the ingestion pipeline

> Status: **v2 (evidence-first investigation), see §19.** v1 misdiagnosed a real IAM failure as DATA_QUALITY. v2 makes the root cause come from extracted facts and verification, not from the model's guess.

## 1. Goal
Every failed or rejected ingestion job publishes an error event to Pub/Sub topic `dfa-ingest-errors` (see `project.md` §7a). `df-agent` treats each event as an **incident**, the way an on-call engineer would:

1. **Gathers evidence:** audit tables, Dataflow job, Cloud Logging, workflow execution, GCS state, quotas.
2. **Finds the root cause** and classifies it.
3. **Decides what to do.**
   - **Auto-act** only when the cause is a simple transient failure: retry the file, at most 2 times.
   - Otherwise it writes an **incident report** (Markdown + PDF, with evidence and proposed code/data fixes) into `df-agent/actions/<incident_id>/` for a human.
4. **Never deletes** anything: no GCP resources, no table data, no files. This is enforced by IAM and by code, not only by prompts.
5. **Reusable:** the core (graph, agents, policy engine, reporting) is domain-agnostic. This pipeline is one "domain pack".

## 2. Decisions
| Topic | Decision |
|---|---|
| Agent framework | **LangGraph** (`StateGraph`), one graph run per incident, SQLite checkpointer (resumable, inspectable) |
| LLM | Configurable per agent role in `config.yaml`: **Ollama** (local, default) or **Claude API** (`ANTHROPIC_API_KEY`). One factory (`init_chat_model`), so switching is config only. |
| Runtime | **Local first**: `make agent-listen` on your Mac pulls `dfa-ingest-errors-sub`. Written so it can later run as a Cloud Run job unchanged. |
| Autonomy | The agent executes **only simple non-code actions** (v1: `RETRY_FILE`). It acts automatically for transient FAILED jobs (max 2 retries per file per 24 h), otherwise only after you approve with `make agent-approve`. Code, config and data changes are always report-only. |
| Models (v1) | Installed Ollama `llama3.2` for all roles, backed by deterministic signature rules (§16). Switch to Claude later via config. |
| Records | `df-agent/actions/<incident_id>/` (report.md, report.pdf, evidence.json, proposed.patch, review.md) **plus** an append-only BigQuery table `file_audit.agent_incidents` (history, retry counting, audit trail) |
| Code changes | **Never applied.** Proposed as a unified diff, reviewed by the code-review agent, saved for a human. |

## 3. Agents and roles
Your six roles, mapped onto a LangGraph workflow, plus two small code-only nodes (intake and executor):

| Agent | Type | What it does | Tools (all read-only unless noted) |
|---|---|---|---|
| **Incident commander** | the graph itself (code) | Routes between agents, enforces loop limits, tracks state. Being code rather than an LLM makes the flow predictable. | — |
| **Audit agent** | LLM + tools | Pulls and summarises what the audit tables say about the job | `get_job(job_id)`, `get_validation_results(job_id)`, `get_file_history(file_name)` (all attempts/generations), `get_recent_failures(contract, hours)`, `get_agent_history(file_name)` (past incidents/retries) |
| **GCP agent** | LLM + tools | Pulls and summarises platform evidence | `get_dataflow_job(id)`, `get_dataflow_errors(id)`, `get_logs(preset, ids, window)` with presets `dataflow_job`, `dataflow_launcher`, `workflow_execution`; `get_workflow_execution(id)`, `get_gcs_object(path)`, `get_quota(region, metric)` |
| **Investigator** | LLM (structured output) | Decides the root cause from all evidence and the brain runbooks. Can ask the gatherers for more evidence (max 2 loops). | none (reasons over evidence) |
| **Action agent** (planner) | LLM (structured output) | Drafts the action plan using the playbook for that root-cause category. Can read repo files to draft a code patch. | `read_repo_file(path)`, `list_repo(dir)` (read-only, inside the repo only) |
| **Code-review agent** | LLM + check | Runs only when a patch is proposed. Checks it applies (`git apply --check` in a temp copy), is minimal, and doesn't touch infra/delete logic. Says which tests are impacted. | `git_apply_check(patch)`, `read_repo_file` |
| **Action-review agent** | **policy engine (code) + LLM** | Approves or rejects each action (section 6) | none |
| **Executor** | code only | Carries out **only** approved auto actions. In v1 the only one is `RETRY_FILE`. | `copy_object` (create-only) |

The "GCP agent" doesn't get a shell or arbitrary `gcloud`. Each operation is a typed Python function using the Google client libraries, with fixed queries and filters. An LLM, especially a small local one, can then only ask *what* to fetch, never *how*. That's safer and much more reliable with Ollama models.

## 4. Workflow
```
 Pub/Sub listener (code, 1 incident at a time)
        │ event (schema v1)
        ▼
   ┌─────────┐
   │ intake  │  parse + validate event · incident_id = job_id · dedupe via agent_incidents
   └────┬────┘  (already handled → ack & stop) · create actions/<id>/
        │ fan-out (parallel)
   ┌────┴─────────────┐
   ▼                  ▼
┌──────────┐    ┌──────────┐
│  audit   │    │   gcp    │   each: bounded tool loop (≤ 6 calls) → evidence items + summary
│  agent   │    │  agent   │
└────┬─────┘    └────┬─────┘
     └──────┬────────┘
            ▼
     ┌──────────────┐  needs more evidence? (≤ 2 rounds) ──► back to audit/gcp with specific asks
     │ investigator │
     └──────┬───────┘  RootCause{category, summary, evidence_refs, confidence}
            ▼
     ┌──────────────┐
     │ action agent │  ActionPlan{actions[]} from playbook for the category
     └──────┬───────┘
            │ has PROPOSE_CODE_CHANGE?
            ├──────── yes ──► ┌──────────────┐
            │                 │ code review  │ verdict + comments
            │ ◄───────────────┘──────────────┘
            ▼
     ┌──────────────────────────────────────┐
     │ action review                         │ 1) policy engine (deterministic)
     │                                       │ 2) LLM reviewer (can only downgrade)
     └──────┬──────────────────────┬────────┘
   auto-approved                needs human
            ▼                      │
     ┌──────────────┐              │
     │  executor    │ RETRY_FILE   │
     └──────┬───────┘              │
            ▼                      ▼
     ┌─────────────────────────────────────┐
     │ report writer → actions/<id>/        │ report.md · report.pdf · evidence.json · proposed.patch · review.md
     └──────┬──────────────────────────────┘
            ▼
     record → file_audit.agent_incidents (append-only) → ack Pub/Sub message
```

**Failure handling.** If any node crashes, the incident is recorded with status `ERROR`, a partial report is written, and the message is **not acked**. Pub/Sub then redelivers it for another attempt. The LangGraph checkpoint lets `make agent-replay` resume or inspect the run.

## 5. Shared state (what flows through the graph)
```python
class IncidentState(TypedDict):
    incident_id: str                 # = job_id
    event: ErrorEvent                # parsed Pub/Sub payload (project.md §7a)
    evidence: list[Evidence]         # {id, source: audit|gcp, kind, summary, data}, appended by gatherers
    evidence_requests: list[str]     # investigator → gatherers ("launcher VM logs around 09:53")
    loops: int                       # evidence rounds used (max 2)
    root_cause: RootCause | None     # category, summary, evidence_refs, confidence
    plan: ActionPlan | None          # actions[] with type, params, rationale
    code_review: CodeReview | None
    decisions: list[ActionDecision]  # per action: AUTO_APPROVED | NEEDS_HUMAN | REJECTED + reasons
    executed: list[ExecutedAction]
    report_paths: dict[str, str]
    status: str                      # OPEN → AUTO_RESOLVED | AWAITING_HUMAN | ERROR
```
Every agent output is a **Pydantic model** via `with_structured_output`. This works with both Ollama (JSON-schema mode) and Claude, and keeps the routing logic in code.

## 6. Action catalog and approval rules
| Action | Meaning | Who can approve |
|---|---|---|
| `RETRY_FILE` | Copy `gs://…/invalid/YYYY/MM/DD/<file>` → `gs://…/landing/<file>`. It's a create-only copy with metadata `retry_of=<job_id>` and `retry_count=n`, so the normal flow processes it as a new generation. | **Auto**, if all auto rules pass. Otherwise human. |
| `PROPOSE_DATA_FIX` | Describes what's wrong in the file (which rows/columns, expected vs actual) and how to correct it | Human (report) |
| `PROPOSE_CODE_CHANGE` | Unified diff and explanation, reviewed by the code-review agent | Human (report) |
| `PROPOSE_CONFIG_CHANGE` | E.g. raise a quota, change `max_concurrent_jobs`, a contract YAML change | Human (report) |
| `ESCALATE` | Unknown or low-confidence cause; report with open questions | Human (report) |
| `REPORT_ONLY` | Nothing to do (e.g. a duplicate or already-resolved incident) | Auto (no side effects) |

**Auto rules for `RETRY_FILE`** (all must hold, from `brain/policies.yaml`):
- event status is `FAILED` (never `REJECTED`: bad data doesn't fix itself)
- root cause category is `TRANSIENT_INFRA` (quota, launcher VM, worker start-up, timeouts, internal or backend errors)
- confidence ≥ 0.7
- fewer than 2 agent retries for this file name in the last 24 h (from `agent_incidents`)
- the source object exists in `invalid/` and `landing/<file>` does not already exist
- no running Dataflow ingestion job is stuck (avoids piling up retries during an outage)

**Review order:**
1. The **policy engine** (code) rejects any action type outside the catalog and any parameter that isn't a path under the landing bucket. It also rejects any plan text containing destructive intent (delete, drop, truncate, overwrite, rm). Then it assigns AUTO or HUMAN per the rules.
2. The **LLM reviewer** checks that the plan is consistent with the evidence. It can turn AUTO into HUMAN (with a reason), but can never turn HUMAN into AUTO.

## 7. Safety: defense in depth
| Layer | Guarantee |
|---|---|
| **IAM** | The agent runs as a new SA `dfa-agent` (impersonated, even locally). Roles: `logging.viewer`, `dataflow.viewer`, `workflows.viewer`, `bigquery.dataViewer` + `jobUser`, `storage.objectViewer`, **`storage.objectCreator` on the landing bucket only** (can create, can't delete or overwrite), `pubsub.subscriber` on the error subscription, `bigquery.dataEditor` **on the `agent_incidents` table only**. Even a fully misbehaving agent cannot delete anything. |
| **Tools** | Typed allowlist per agent. No shell, no free-form SQL, no generic HTTP. |
| **Policy engine** | Deterministic gate on every action (section 6) |
| **LLM reviewer** | Second opinion, downgrade-only |
| **Code** | Patches are written to files and never applied to the repo |
| **Executor** | Implements only create-only copy. There's no delete or move code path to call. |

## 8. Incident report (what a human gets)
`df-agent/actions/<incident_id>/`:
| File | Content |
|---|---|
| `report.pdf` / `report.md` | Title, severity, status; **summary** (what happened, impact: file, table, rows); **timeline** (landed → registered → Dataflow → failed); **root cause** with confidence and cited evidence; **failed checks** table; **actions**: what was done automatically and what needs a human, step by step; **proposed code change** (diff with review comments); **how to verify**; links (Dataflow job, workflow execution, audit SQL) |
| `evidence.json` | Every evidence item the agents collected (raw + summary) |
| `proposed.patch` | Unified diff, if any (`git apply` it yourself after review) |
| `review.md` | Policy and LLM reviewer decisions per action, plus the code review |

BigQuery `file_audit.agent_incidents` (append-only, one row per state change): `incident_id, job_id, event_status, status, category, confidence, root_cause, actions (JSON), decisions (JSON), executed_actions (JSON), retry_of_job_id, retry_count, report_path, llm_provider, llm_model, created_at`.

## 9. The brain: recommended strategy
Your `brian/` folder (I propose renaming it to `brain/`) is the right idea. To make it work well, especially with local models and small context windows:

1. **Keep it structured and small; retrieve by key rather than RAG.** The event carries `error_code` and `failed_checks[].check_name`, so the graph loads exactly the matching runbook(s). No vector store is needed, and it's predictable.
2. **Layout:**
   ```
   brain/
     system.md              pipeline architecture for agents (flow, components, statuses, where things live), ~1-2 pages
     schemas/               job_metadata.json, validation_metadata.json, agent_incidents.json (+ column semantics)
     runbooks/              one per error: symptoms → how to confirm → likely causes → recommended action
       QUOTA_EXCEEDED.md  DATAFLOW_JOB_FAILED.md  RECON_MISMATCH.md
       header.md  trailer.md  delimiter.md  data_type.md  file_name.md  contract_match.md
     playbooks.yaml         category → allowed action types + templates
     signatures.yaml        known error patterns → category + confidence (deterministic, see §16)
     policies.yaml          action catalog, auto rules, forbidden verbs, limits
     glossary.md            status values, check names, error codes
   ```
3. **Avoid drift.** `brian/project.md` and the schema JSONs are copies, and they'll go stale. I propose `make agent-brain-sync`, which copies `infra/bq/*.json` into `brain/schemas/` and checks that `system.md` references real checks and error codes. A unit test fails if a check name in the pipeline has no runbook.
4. **Write runbooks for the agent:** short and imperative, each with a "how to confirm" list that maps to tool names (e.g. "call `get_dataflow_errors`; look for `QUOTA_EXCEEDED` / `IN_USE_ADDRESSES`").

I'll draft `system.md`, all the runbooks, `playbooks.yaml` and `policies.yaml` from what we built, for you to review.

## 10. Reusability: generic core vs domain pack
```
df-agent/
  df-agent.md                    this document
  config.yaml                    LLM per role, limits, paths, which domain pack to load
  brain/                         domain knowledge (section 9)
  incident_core/                 GENERIC: no pipeline-specific code
    state.py                     IncidentState, Evidence, RootCause, ActionPlan, … (Pydantic)
    graph.py                     builds the StateGraph from a DomainPack
    llm.py                       provider factory (ollama | anthropic) per role
    policy.py                    loads policies.yaml, evaluates plans
    agents/                      investigator, action_planner, action_reviewer, code_reviewer, gatherer (generic tool-loop agent)
    sources/pubsub.py            listener (flow control = 1, ack after record)
    store/                       bigquery.py, local.py (incident records)
    report/                      markdown.py (Jinja), pdf.py (reportlab)
  domains/dataflow_ingest/       DOMAIN PACK for this pipeline
    pack.py                      DomainPack: event parser, gatherer tool sets, executor, brain path
    event_parser.py              ErrorEvent v1 → state
    tools_audit.py               BigQuery read tools
    tools_gcp.py                 Dataflow / Logging / Workflows / GCS / quota read tools
    executor.py                  RETRY_FILE implementation
  actions/                       outputs (git-ignored)
  tests/
```
Adding another incident domain (e.g. a different pipeline, or Cloud Run errors) means writing a new `domains/<name>/` with a parser, tools, an executor and a brain. The graph, agents, policy engine, reporting and listener are reused unchanged.

## 11. LLM configuration
```yaml
# df-agent/config.yaml
llm:
  default:   {provider: ollama, model: "llama3.2", base_url: "http://localhost:11434", temperature: 0}
  roles:                                   # optional per-role overrides, e.g. once Claude is enabled:
    # investigator:   {provider: anthropic, model: "claude-sonnet-5-5"}
    # action_planner: {provider: anthropic, model: "claude-sonnet-5-5"}
    # code_reviewer:  {provider: anthropic, model: "claude-opus-5-5"}
features: {generate_patches: false}        # true only with a strong model (see §16)
limits: {gatherer_tool_calls: 6, evidence_rounds: 2, max_retries_per_file_24h: 2}
domain: dataflow_ingest
```
- **Ollama:** v1 uses the installed `llama3.2` (3B) for every role. It supports tool calling and JSON output; its weak reasoning is compensated by signature rules (§16). `llava` is a vision model and isn't used.
- **Claude:** set `ANTHROPIC_API_KEY` and switch `provider: anthropic` for any role. A good mix is local models for gatherers and Claude for the investigator and code review.
- Each incident records which provider and model each role used (`agent_incidents.llm_*`).

## 12. Infra and Makefile additions
- **Terraform** (`infra/agent.tf`): SA `dfa-agent` + IAM (section 7), table `file_audit.agent_incidents` (`infra/bq/agent_incidents.json`), and your user's `iam.serviceAccountTokenCreator` on `dfa-agent` (for local impersonation). All removed by `make gcs-destroy`.
- **Make targets:**
  | Target | Action |
  |---|---|
  | `make agent-venv` | Separate venv for df-agent (LangGraph and the Google clients don't mix with the Beam pins) |
  | `make agent-listen` | Long-running listener, one incident at a time, Ctrl-C to stop |
  | `make agent-once` | Process at most one pending error event, then exit |
  | `make agent-approve INCIDENT=…` | Shows the incident's pending non-code actions (v1: `RETRY_FILE`), asks you to confirm each, executes them via the executor, records `APPROVED_BY_HUMAN` |
  | `make agent-replay JOB_ID=…` | Run the graph for a job_id without Pub/Sub (rebuilds the event from the audit tables), for testing and re-investigation |
  | `make agent-brain-sync` | Refresh `brain/schemas` from `infra/bq` and run the brain consistency checks |
  | `make agent-test` | Unit and scenario tests (no GCP, fake LLM) |

## 13. Testing strategy
- **Unit tests:**
  - **Policy engine:** a full allow/deny matrix, including plans that try to delete, overwrite, use paths outside landing, or carry unknown action types. Every one must be blocked.
  - **Event parser:** against real `make errors` samples.
  - **Tools:** with faked clients.
- **Scenario tests:** the whole graph runs with a scripted fake LLM.
  - Quota failure leads to an auto retry.
  - A `header` REJECTED event leads to a report with `PROPOSE_DATA_FIX` and no execution.
  - A Python traceback leads to `PROPOSE_CODE_CHANGE`, then the code review, then the report.
  - A low-confidence event leads to `ESCALATE`.
  - A third retry is refused by the policy.
- **End-to-end (you run these):**
  - Upload `mockdata/user_20261016.csv`. You should get a REJECTED event, a report in `actions/<job_id>/` and a BigQuery row.
  - Replay the earlier `QUOTA_EXCEEDED` job with `make agent-replay`. The agent should copy the file back to `landing/`, which starts a new, successful job.

## 14. Delivery phases
| Phase | Content | Exit criteria |
|---|---|---|
| 0 | Brain: `system.md`, runbooks, playbooks, policies (for your review) | You approve the brain content |
| 1 | `incident_core` skeleton: state, LLM factory, policy engine and tests; Terraform SA + table | `make agent-test` passes the policy matrix |
| 2 | Domain pack tools (audit + gcp) + `agent-replay`, gatherers only; evidence written to `evidence.json` | Replay of a real job produces sensible evidence |
| 3 | Investigator, action planner, reviewers, report writer (MD + PDF) | Replay produces a complete report for REJECTED and FAILED cases |
| 4 | Executor (`RETRY_FILE`) + `make agent-approve` + Pub/Sub listener + `agent_incidents` records | End-to-end tests in section 13 pass |
| Later | Switch reasoning roles to Claude + enable patch generation; Cloud Run deployment; notifications (email/Slack) | — |

## 15. Decisions log / open questions
| # | Question | Answer |
|---|---|---|
| 1 | Rename `brian/` → `brain/` | **Yes** (run `mv df-agent/brian df-agent/brain`) |
| 2 | Ollama model | **Use the installed `llama3.2` (3B) for all roles** (limited RAM). Switch to the Claude API once the agent works end to end; it's a config change only. Because a 3B model is weak at reasoning, the design adds **deterministic signature rules** (§16) so known errors are classified correctly even if the LLM struggles. |
| 3 | `make agent-approve` in v1? | **Yes, in v1.** The agent may execute **simple, non-code actions** (v1: `RETRY_FILE`): automatically when the auto rules pass, otherwise after you approve with `make agent-approve`. Code, config and data changes are never executed; they're report-only. |
| 4 | Severity | **Yes, simple rule for now:** `FAILED` = **SEV2** (pipeline/platform problem, may affect other files); `REJECTED` = **SEV3** (one bad file, data owner must fix); raised one level if the same contract fails 3+ times in 24 h. |

**What `make agent-approve INCIDENT=<id>` would do.** When the policy engine says "needs human" for an action the agent *could* execute itself, the agent only writes it into the report. The main case is a `RETRY_FILE` that failed an auto rule: a 3rd retry, confidence below 0.7, or a FAILED job not clearly transient. `agent-approve` would:
1. show the pending actions from `actions/<id>/review.md`;
2. let you confirm each one;
3. execute them through the same executor (still create-only copies, still under the `dfa-agent` SA);
4. append an `APPROVED_BY_HUMAN` row (with your user) to `agent_incidents`.

It **doesn't** apply code patches or data fixes; those always stay manual.

## 16. Running on a small local model (llama3.2, 3B)
A 3B model can summarise and fill simple typed fields, but it isn't reliable at multi-step reasoning or code. So for v1:
1. **Signature rules first.** `brain/signatures.yaml` maps known evidence patterns to a root-cause category with a fixed confidence. Examples: `QUOTA_EXCEEDED|RESOURCE_EXHAUSTED|IN_USE_ADDRESSES` → `TRANSIENT_INFRA` (0.9); event status `REJECTED` + failed check `header|trailer|delimiter|data_type|file_name|contract_match` → `DATA_QUALITY` (0.95); `Traceback` in Dataflow errors → `CODE_BUG` (0.6). The investigator receives the rule match as a **hint**. If the LLM output is invalid, or it contradicts a high-confidence rule without evidence, the **rule wins** and the report flags it.
   **Signal vs context (important).** Signatures only match **failure-signal** text: the event's own error text,
   plus evidence from tools marked `signal=True` (`get_job`, `get_validation_results`, `get_dataflow_errors`,
   `get_logs`, `get_workflow_execution`). Context evidence (`get_quota`, `get_dataflow_job`, `get_gcs_object`, file and
   agent history, other jobs) is shown to the LLM but **never** triggers a signature. Signature patterns must describe
   an *error* (`QUOTA_EXCEEDED`, `Quota … exceeded`), never just name a resource. *(Bug fixed 2026-10-18: the bare
   pattern `IN_USE_ADDRESSES` matched the agent's own quota reading, so every FAILED job was classified QUOTA_EXCEEDED,
   and that misclassification then overrode the LLM.)* The investigator also extracts the **key error lines** (e.g.
   `UnicodeDecodeError: …`) from signal text, puts them first in its prompt, and lists them in the report with their
   evidence id.
2. **LLM where it adds value:** evidence summaries, the human-readable root-cause narrative, and filling the report. Gatherers mostly run a fixed default set of tools; the LLM only adds optional extra lookups.
3. **Structured-output guard:** each LLM call gets one retry with the validation error. If it still fails, the node falls back to the deterministic result and marks the incident `low_llm_confidence`. A `CODE_BUG` with no usable patch becomes `ESCALATE`.
4. **Code patches:** with the 3B model the planner only *describes* the code change (file, function, what to change). Full diffs are generated only when the `action_planner` / `code_reviewer` roles are switched to Claude. Config gate: `features.generate_patches: false` by default.
5. **Small prompts:** only the matching runbook(s) and a trimmed evidence summary go into each prompt, never the whole brain.

Switching to Claude later: set `ANTHROPIC_API_KEY`, change `provider: anthropic` in `config.yaml`, and set `features.generate_patches: true`. Nothing else changes.

## 17. Running it (v1)
```bash
make gcs-setup              # creates the dfa-agent SA, its IAM, file_audit.agent_incidents; lets your gcloud user impersonate it
make agent-venv             # .venv-agent (LangGraph, Google clients, reportlab, …)
make agent-test             # policy matrix, signatures, brain coverage, graph scenarios (no GCP, no LLM)
make agent-brain-sync       # copy BQ schemas into brain/schemas + brain consistency check
ollama serve                # in another terminal, if Ollama isn't already running (model: llama3.2)
make agent-replay JOB_ID=<a FAILED or REJECTED job_id>   # investigate an existing job without Pub/Sub
make agent-listen           # handle new error events as they arrive (Ctrl-C to stop)
make agent-approve INCIDENT=<job_id>                     # approve pending retries for an incident
make agent-graph            # draw the LangGraph workflow → df-agent/graph.mmd (offline; PNG=1 also renders graph.png via mermaid.ink)
```
`agent-graph` compiles the real graph with the configured domain pack (GCP clients are lazy, so no GCP calls) and exports it with LangGraph's `get_graph().draw_mermaid()`. Every conditional edge in `graph.py` declares its possible targets (`path_map`) so the drawing shows the real branches, not an edge to every node. View `graph.mmd` in VS Code's Mermaid preview or paste it into https://mermaid.live.
Outputs: `df-agent/actions/<job_id>/` (`report.pdf`, `report.md`, `evidence.json`, `review.md`, `state.json`, `proposed.patch` when patches are enabled), plus one row per run in `file_audit.agent_incidents`. `actions/` and `.state/` are git-ignored.

**Code map:**
| Path | What |
|---|---|
| `incident_core/graph.py` | LangGraph wiring: intake, gatherers, investigator, planner, code review, action review, executor, report, record |
| `incident_core/agents/` | gatherer (generic tool-loop agent), investigator, planner, reviewers |
| `incident_core/policy.py` | deterministic policy engine (`brain/policies.yaml`) |
| `incident_core/signatures.py` | rule-based root-cause hints (`brain/signatures.yaml`) |
| `incident_core/report/` | Jinja Markdown template + reportlab PDF |
| `incident_core/runner.py` | CLI: `listen`, `once`, `replay`, `approve`, `brain-check`, `graph` |
| `domains/dataflow_ingest/` | event parsing, audit + GCP tools, `RETRY_FILE` executor, report sections |
| `infra/agent.tf` | `dfa-agent` SA + least-privilege IAM + `agent_incidents` table |

## 18. Not-an-incident handling and the investigation trace
**Not an incident.** Intake first asks the domain pack `incident_check(event)`. A job that is `SUCCESS`, still `IN_PROGRESS`, or `DUPLICATE_SKIPPED` is closed immediately as **`NO_INCIDENT`**: no gatherers and no LLM calls. The report says "Not an incident: no action required", with the facts (rows loaded, validation, recon, archive path), and the run is recorded with `status = NO_INCIDENT`. This mostly applies to `agent-replay`, since successful jobs never reach the error topic.

**Investigation trace.** Every step is logged to the console and saved per incident:
```
19:42:25 INFO  ====================================================================================
19:42:25 INFO  INCIDENT f47c2e49-… | REJECTED user_load user_20261016.csv | trace: …/trace.jsonl
19:42:25 INFO  [f47c2e49] intake           ▶ intake started
19:42:25 INFO  [f47c2e49] intake           incident confirmed, severity SEV3; starting investigation
19:42:26 INFO  [f47c2e49] audit_agent      get_job(job_id=f47c…) [default] → audit-1: job …: status=REJECTED … (0.8s)
19:42:35 INFO  [f47c2e49] audit_agent      LLM #1 choose_tools [llama3.2] attempt 1: ok (9.1s)
19:42:41 INFO  [f47c2e49] investigator     signatures matched: rejected_header→DATA_QUALITY (0.95)
19:42:55 WARN  [f47c2e49] investigator     LLM #4 root_cause [llama3.2] attempt 1: INVALID (…) (12.1s)
19:43:05 INFO  [f47c2e49] investigator     root cause DATA_QUALITY (confidence 0.95, via signature)
19:43:30 INFO  [f47c2e49] action_reviewer  policy: PROPOSE_DATA_FIX → REPORT (manual action for a human …)
19:44:51 INFO  RESULT f47c2e49 | status AWAITING_HUMAN | root cause DATA_QUALITY (0.95) | executed nothing | 9 LLM call(s) | 146.0s
```
| Output | Content |
|---|---|
| `actions/<id>/trace.jsonl` | One JSON line per step: timestamp, elapsed, node, kind (start/end/tool/llm/signature/decision/execution/note/error), message, duration, data |
| `actions/<id>/trace.md` | The same as a readable table; also embedded in the report as "Investigation trace" |
| `actions/<id>/llm/NN_<role>_<purpose>.json` | Each LLM call's system prompt, user prompt, output or validation error, model and duration (`logging.save_llm_io`) |

Replaying an incident keeps the previous `trace.jsonl` / `llm/` with a timestamp suffix. `logging.level: DEBUG` in `config.yaml` brings back library logs (httpx etc.).

**Things to check on the first real run:**
- The Ollama server is running and `llama3.2` responds. If the LLM fails, the agent falls back to rules and says so under "Agent notes" in the report.
- Impersonation works: the first GCP call fails with `iam.serviceAccounts.getAccessToken` denied if the token-creator binding hasn't propagated yet (wait a minute).
- `brain/` still has the old top-level copies (`project.md`, `job_metadata.json`, `validation_metadata.json`). The agent doesn't read them; `brain/schemas/` (from `make agent-brain-sync`) replaces them, so you can delete them.

## 19. v2: evidence-first investigation (after the IAM misdiagnosis)
**What went wrong in v1** (incident `ba0fbdb5`: the Dataflow SA had lost `bigquery.dataEditor`):
| # | Failure | v2 fix |
|---|---|---|
| 1 | Tracebacks were cut to their **first** 3,000 chars and evidence to its first 6,000. The real error (`Access Denied … Permission bigquery.tables.create denied … 403`) is at the **bottom** of a traceback and was lost. | Messages are fetched in full. Long strings are clipped **head + tail** (the tail holds the exception), per string rather than per blob. |
| 2 | The key-error regex ran on double-escaped JSON and found only the Beam `Workflow failed` wrapper. | New deterministic **log analyst** node (`incident_core/errors.py`) parses raw texts into an **error digest** (below). |
| 3 | The permission signature didn't match BigQuery wording, and a generic `python_traceback` (0.8) matched every Dataflow failure. | Signatures run on the digest, which is normalised (e.g. `PERMISSION_DENIED http=403 permission=bigquery.tables.create`). The permission, not-found, schema and BigQuery-quota signatures are broadened. A generic traceback is only a weak hint (0.55); CODE_BUG at 0.8 needs a frame from **our** code (`/pipeline/…py`). |
| 4 | The LLM said DATA_QUALITY with 0 failed checks and nothing stopped it. | **Consistency guards** reject impossible answers (below). |
| 5 | Small-model gatherer summaries added ~80 s and no information. | Gatherers are deterministic. The LLM tool loop runs only when the investigator asks for something specific; summaries are off by default. |
| 6 | The planner LLM invented action type `CREATE`. | Output is constrained to the plan's own action types (dynamic `Literal`). **Remediation steps are deterministic templates** filled from the digest (permission, resource, principal). |

**New flow:** `intake → gatherers → log_analyst → investigator ⇄ gatherers → planner → reviewers → executor → report → record`

**Error digest** (log analyst, no LLM). For every error text in this job's signal evidence:
- **Exception chain:** the *last* exception of each traceback, plus `During handling…` / wrapped errors (e.g. `RuntimeError` → `HttpForbiddenError`).
- **Google API error fields:** HTTP code, gRPC status, reason, message (BigQuery/GCS JSON payloads embedded in exception text).
- **Permission and resource:** e.g. `Permission bigquery.tables.create denied on dataset bigquerytarun:raw`.
- **Failing Dataflow step:** from `Workflow failed. Causes: S56:LoadToTargetTable/WriteToBigQuery/...`.
- **Grouping:** duplicates (Dataflow retries a failing bundle 4×) are grouped with a count and first-seen time.
- **Class:** `permission | not_found | quota | transient | encoding | schema | exception`. The **primary error** is the most specific one: an HTTP/permission error beats a bare exception, which beats the framework wrapper.

**Hypothesis verification.** When a signature matches, the domain pack can run read-only checks that **confirm** it. For a permission error, the agent:
- reads the Dataflow job's worker service account (`get_dataflow_job`);
- lists that account's project roles (`get_principal_roles`, Resource Manager IAM policy);
- reads the target dataset's access entries (`get_dataset_access`).

The report can then say *"dfa-dataflow@… has roles [dataflow.worker, bigquery.jobUser, …]; none grants bigquery.tables.create on raw"*. This needs two extra read-only roles on `dfa-agent`: `roles/iam.securityReviewer` (read IAM policies) and `roles/bigquery.metadataViewer` (dataset metadata, no table data).

**Consistency guards** (code, applied to the LLM's answer):
| Rule | Effect |
|---|---|
| `DATA_QUALITY` needs status REJECTED or at least one failed validation check | otherwise the answer is rejected and the signature or UNKNOWN is used |
| A high-confidence signature (≥ 0.85) on the primary error | decides the category; the LLM can only explain it |
| `TRANSIENT_INFRA` without a transient signature | confidence capped at 0.6, so it is **never auto-retried on the model's say-so** |
| No valid evidence id cited | confidence × 0.6 |

**CONFIG incidents** now also plan a `RETRY_FILE`. It needs a human (`make agent-approve`) after the config fix, because policy auto-approves retries only for TRANSIENT_INFRA.

**Evaluation harness (measure, don't guess).** `make agent-eval-add INCIDENT=<id> EXPECTED=<CATEGORY>` saves a real incident's event + evidence to `df-agent/evals/cases/`. `make agent-eval` replays every case through log analyst + investigator with the configured model, then prints accuracy and a confusion table. Use it to compare llama3.2 against Claude and to keep fixes from regressing.

**Pipeline side:** `publish_failure` now keeps the **tail** of each Dataflow error message (the exception), not the head.
