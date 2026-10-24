"""Evaluation harness: replay recorded incidents through log_analyst + investigator and measure accuracy.

  eval-add --incident <id> --expected CONFIG [--note "..."]   save actions/<id>/ (event + evidence) as a case
  eval [--live]                                               run every case with the configured model(s)

Offline by default: the recorded evidence is used as-is and no GCP call is made (verification tools are skipped
unless --live). This is how you compare llama3.2 against Claude, and keep fixes from regressing.
"""

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from incident_core.agents.investigator import make_investigator_node
from incident_core.agents.log_analyst import make_log_analyst_node
from incident_core.brain import Brain
from incident_core.models import CATEGORIES

# Evidence recorded before the signal flag existed: infer it from the tool that produced it.
_SIGNAL_KINDS = {"get_job", "get_validation_results", "get_dataflow_errors", "get_logs", "get_workflow_execution"}


def cases_dir(cfg: dict) -> Path:
    path = Path(cfg["actions_dir"]).parent / "evals" / "cases"
    path.mkdir(parents=True, exist_ok=True)
    return path


def add_case(cfg: dict, incident_id: str, expected: str, note: str = "") -> Path:
    expected = expected.upper()
    if expected not in CATEGORIES + ["NO_INCIDENT"]:
        raise ValueError(f"expected must be one of {CATEGORIES + ['NO_INCIDENT']}")
    folder = Path(cfg["actions_dir"]) / incident_id
    state = json.loads((folder / "state.json").read_text())
    ev = json.loads((folder / "evidence.json").read_text())
    # Only gathered evidence; analysis/verification items are recomputed by the eval.
    ev = [e for e in ev if e["source"] not in ("analysis", "verify") and e["kind"] != "summary"]
    case = {"incident_id": incident_id, "expected_category": expected, "note": note,
            "added_at": datetime.now(timezone.utc).isoformat(), "event": state["event"], "evidence": ev}  # fmt: skip
    path = cases_dir(cfg) / f"{incident_id}.json"
    path.write_text(json.dumps(case, indent=2, default=str))
    return path


class _OfflinePack:
    """Wraps the domain pack so evals never touch live systems unless asked."""

    def __init__(self, pack, live: bool):
        self._pack, self._live = pack, live

    def __getattr__(self, name):
        return getattr(self._pack, name)

    def verification_calls(self, *args):
        return self._pack.verification_calls(*args) if self._live else []


def run_eval(cfg: dict, pack, live: bool = False) -> dict:
    brain = Brain(cfg["brain_dir"])
    analyst = make_log_analyst_node(cfg, brain, _OfflinePack(pack, live))
    investigator = make_investigator_node(cfg, brain)
    rows, confusion = [], Counter()

    for path in sorted(cases_dir(cfg).glob("*.json")):
        case = json.loads(path.read_text())
        evidence = [{**e, "signal": e.get("signal", e["kind"] in _SIGNAL_KINDS)} for e in case["evidence"]]
        state = {"incident_id": case["incident_id"], "event": case["event"], "evidence": evidence, "loops": 0}
        out = analyst(state)
        state.update(out, evidence=evidence + out["evidence"])
        rc = investigator(state)["root_cause"]
        ok = rc["category"] == case["expected_category"]
        confusion[(case["expected_category"], rc["category"])] += 1
        rows.append({"case": case["incident_id"][:8], "expected": case["expected_category"], "got": rc["category"],
                     "confidence": rc["confidence"], "method": rc["method"], "ok": ok,
                     "primary_error": (rc.get("primary_error") or {}).get("kind"), "note": case.get("note", "")})  # fmt: skip

    accuracy = sum(r["ok"] for r in rows) / len(rows) if rows else 0.0
    result = {"ran_at": datetime.now(timezone.utc).isoformat(), "accuracy": accuracy, "cases": rows,
              "confusion": {f"{e}->{g}": n for (e, g), n in confusion.items()},
              "models": {role: cfg["llm"].get("roles", {}).get(role) or cfg["llm"]["default"] for role in ("investigator",)}}  # fmt: skip
    out_dir = cases_dir(cfg).parent / "results"
    out_dir.mkdir(exist_ok=True)
    (out_dir / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}.json").write_text(json.dumps(result, indent=2, default=str))
    return result


def print_eval(result: dict) -> None:
    print(f"\n{'case':10} {'expected':16} {'got':16} {'conf':>5}  {'method':24} primary   ok")
    for r in result["cases"]:
        print(f"{r['case']:10} {r['expected']:16} {r['got']:16} {r['confidence']:5.2f}  {r['method']:24} "
              f"{(r['primary_error'] or '-'):9} {'✔' if r['ok'] else '✘'}")  # fmt: skip
    print(f"\naccuracy: {result['accuracy']:.0%} over {len(result['cases'])} case(s); confusion: {result['confusion']}")
