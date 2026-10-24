"""CLI: listen | once | replay | approve | brain-check.

  python -m incident_core.runner listen            # long-running, one incident at a time
  python -m incident_core.runner once              # at most one pending event
  python -m incident_core.runner replay --ref JOB  # rebuild the event from the audit tables and run the graph
  python -m incident_core.runner approve --incident JOB
  python -m incident_core.runner brain-check
"""

import argparse
import getpass
import importlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from incident_core.brain import Brain
from incident_core.config import load_config
from incident_core.graph import build_graph, sqlite_checkpointer
from incident_core.models import NEEDS_HUMAN
from incident_core.policy import PolicyEngine
from incident_core.store import CompositeStore, LocalIncidentStore
from incident_core.trace import end_trace, start_trace

log = logging.getLogger("df-agent")


def load_pack(cfg: dict):
    module = importlib.import_module(f"domains.{cfg['domain']}.pack")
    return module.create_pack(cfg)


def make_store(cfg: dict, pack):
    primary = pack.incident_store() if hasattr(pack, "incident_store") else None
    store = CompositeStore(primary, LocalIncidentStore(cfg["state_dir"]))
    if hasattr(pack, "attach_store"):
        pack.attach_store(store)
    return store


def run_incident(cfg, pack, store, event: dict, force: bool = False) -> dict:
    incident_id = event["incident_id"]
    graph = build_graph(cfg, pack, store, checkpointer=sqlite_checkpointer(cfg))
    thread = f"{incident_id}:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
    trace = start_trace(incident_id, str(Path(cfg["actions_dir"]) / incident_id), cfg["logging"].get("save_llm_io", True))
    log.info("=" * 100)
    log.info("INCIDENT %s | %s | trace: %s", incident_id, event.get("title"), trace.path)
    log.info("=" * 100)
    started = time.monotonic()
    try:
        result = graph.invoke(
            {"incident_id": incident_id, "event": event, "force": force},
            config={"configurable": {"thread_id": thread}, "recursion_limit": 60},
        )
        rc = result.get("root_cause") or {}
        executed = [f"{e['action_type']}:{e['status']}" for e in result.get("executed") or []]
        log.info("-" * 100)
        log.info("RESULT %s | status %s | root cause %s (%s) | executed %s | %d LLM call(s) | %.1fs",
                 incident_id[:8], result.get("status"), rc.get("category", "-"),
                 f"{rc['confidence']:.2f}" if rc.get("confidence") is not None else "-", executed or "nothing",
                 trace.llm_calls, time.monotonic() - started)  # fmt: skip
        if result.get("report_paths"):
            log.info("REPORT %s", result["report_paths"]["report_pdf"])
        log.info("-" * 100)
        return result
    finally:
        end_trace()


def record_error(cfg, store, event: dict, exc: Exception):
    store.record({"incident_id": event.get("incident_id", "unknown"), "subject": event.get("subject"),
                  "event_status": event.get("status"), "status": "ERROR", "root_cause": f"agent error: {exc}",
                  "actor": "df-agent"})  # fmt: skip


def cmd_pull(cfg, pack, store, once: bool):
    from google.api_core.exceptions import DeadlineExceeded
    from google.cloud import pubsub_v1

    subscriber = pack.subscriber() if hasattr(pack, "subscriber") else pubsub_v1.SubscriberClient()
    sub = subscriber.subscription_path(cfg["gcp"]["project"], cfg["gcp"]["error_subscription"])
    log.info("listening on %s (Ctrl-C to stop)", sub)
    while True:
        try:
            response = subscriber.pull(request={"subscription": sub, "max_messages": 1}, timeout=30)
        except DeadlineExceeded:
            response = None
        if response is None or not response.received_messages:
            if once:
                log.info("no pending error events")
                return
            continue
        received = response.received_messages[0]
        # Keep the lease while the agents work (max 600 s); duplicates are deduped via the incident store.
        subscriber.modify_ack_deadline(request={"subscription": sub, "ack_ids": [received.ack_id], "ack_deadline_seconds": 600})
        message = received.message
        try:
            payload = json.loads(message.data.decode("utf-8"))
            event = pack.parse_event(payload, dict(message.attributes))
        except Exception as exc:  # poison message: keep a copy, ack so it doesn't loop forever
            bad = Path(cfg["actions_dir"]) / "_unparseable"
            bad.mkdir(parents=True, exist_ok=True)
            (bad / f"{message.message_id}.json").write_bytes(message.data)
            log.error("unparseable event %s saved to %s: %s", message.message_id, bad, exc)
            subscriber.acknowledge(request={"subscription": sub, "ack_ids": [received.ack_id]})
            continue
        try:
            run_incident(cfg, pack, store, event)
            subscriber.acknowledge(request={"subscription": sub, "ack_ids": [received.ack_id]})
        except Exception as exc:
            log.exception("incident %s failed; message will be redelivered", event["incident_id"])
            record_error(cfg, store, event, exc)
            subscriber.modify_ack_deadline(request={"subscription": sub, "ack_ids": [received.ack_id], "ack_deadline_seconds": 120})
            time.sleep(5)
        if once:
            return


def cmd_replay(cfg, pack, store, ref: str):
    run_incident(cfg, pack, store, pack.event_from_reference(ref), force=True)


def cmd_approve(cfg, pack, store, incident_id: str, assume_yes: bool):
    path = Path(cfg["actions_dir"]) / incident_id / "state.json"
    if not path.exists():
        sys.exit(f"No incident snapshot at {path}")
    snapshot = json.loads(path.read_text())
    engine = PolicyEngine(Brain(cfg["brain_dir"]).policies, pack.config_values())
    pending = [(a, d) for a, d in zip(snapshot["plan"], snapshot["decisions"]) if d["decision"] == NEEDS_HUMAN]
    if not pending:
        print("Nothing awaiting approval for this incident.")
        return

    user = getpass.getuser()
    executed = []
    for action, decision in pending:
        facts = pack.policy_facts(action, snapshot["event"], snapshot["root_cause"])
        blockers = engine.recheck_for_human(action, facts)
        print(f"\n{action['type']}  params={json.dumps(action.get('params', {}))}\n  {action.get('description', '')}")
        print(f"  agent decision: {'; '.join(decision['reasons'])}")
        if blockers:
            print(f"  BLOCKED by policy now: {'; '.join(blockers)}")
            continue
        answer = "y" if assume_yes else input("  Execute? [y/N] ").strip().lower()
        if answer != "y":
            print("  skipped")
            continue
        record = {"action_type": action["type"], "params": action.get("params", {}), "actor": user,
                  "at": datetime.now(timezone.utc).isoformat()}  # fmt: skip
        try:
            record.update(status="DONE", result=pack.execute(action, incident_id))
        except Exception as exc:
            record.update(status="FAILED", result={"error": str(exc)})
        print(f"  {record['status']}: {record['result']}")
        executed.append(record)

    if executed:
        snapshot.setdefault("approvals", []).extend(executed)
        path.write_text(json.dumps(snapshot, indent=2, default=str))
        with (path.parent / "review.md").open("a") as f:
            for e in executed:
                f.write(f"- Human approval by {user}: {e['action_type']} → {e['status']} {e['result']}\n")
        store.record({
            "incident_id": incident_id,
            "subject": snapshot["event"].get("subject"),
            "event_status": snapshot["event"].get("status"),
            "status": "APPROVED_BY_HUMAN",
            "category": snapshot["root_cause"].get("category"),
            "executed_actions": executed,
            "executed_retry_count": sum(1 for e in executed if e["action_type"] == "RETRY_FILE" and e["status"] == "DONE"),
            "actor": user,
        })  # fmt: skip


def cmd_brain_check(cfg) -> int:
    """Every signature runbook and every runbook hint the pack can emit must exist."""
    brain = Brain(cfg["brain_dir"])
    keys = set(brain.runbook_keys())
    problems = [f"signature {s['id']} → missing runbook {s['runbook']}" for s in brain.signatures
                if s.get("runbook") and s["runbook"] not in keys]  # fmt: skip
    for category in ("TRANSIENT_INFRA", "DATA_QUALITY", "CODE_BUG", "CONFIG", "UNKNOWN"):
        if category not in brain.playbooks:
            problems.append(f"playbook missing for {category}")
    for p in problems:
        print("PROBLEM:", p)
    print("brain OK" if not problems else f"{len(problems)} problem(s)")
    return 1 if problems else 0


def cmd_graph(cfg, pack, out: str, png: bool):
    """Export the compiled workflow as Mermaid (offline); PNG rendering uses the mermaid.ink web API."""
    drawable = build_graph(cfg, pack, LocalIncidentStore(cfg["state_dir"])).get_graph()
    mermaid = drawable.draw_mermaid()
    path = Path(out)
    path.write_text(mermaid)
    print(mermaid)
    print(f"wrote {path.resolve()}")
    if png:
        png_path = path.with_suffix(".png")
        drawable.draw_mermaid_png(output_file_path=str(png_path))
        print(f"wrote {png_path.resolve()}")


def configure_logging(cfg: dict):
    """Compact console output: the investigation trace is the main signal; library chatter is hidden."""
    settings = cfg.get("logging", {})
    logging.basicConfig(level=getattr(logging, settings.get("level", "INFO")),
                        format="%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S")  # fmt: skip
    for noisy in ("httpx", "httpcore", "google", "urllib3", "langchain", "langgraph"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    # Each LLM call is already traced (role, purpose, duration, validity); hide the raw HTTP request lines.
    logging.captureWarnings(True)
    logging.getLogger("py.warnings").setLevel(logging.ERROR)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="df-agent")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("listen")
    sub.add_parser("once")
    replay = sub.add_parser("replay")
    replay.add_argument("--ref", required=True, help="domain reference, e.g. a job_id")
    approve = sub.add_parser("approve")
    approve.add_argument("--incident", required=True)
    approve.add_argument("--yes", action="store_true", help="approve every pending action without prompting")
    sub.add_parser("brain-check")
    graph = sub.add_parser("graph", help="export the agent workflow as a Mermaid diagram")
    graph.add_argument("--out", default="graph.mmd")
    graph.add_argument("--png", action="store_true", help="also render a PNG (sends the diagram text to mermaid.ink)")
    ev =sub.add_parser("eval", help="replay recorded incident cases and report accuracy")
    ev.add_argument("--live", action="store_true", help="also run live verification tools (GCP calls)")
    ev_add = sub.add_parser("eval-add", help="save an investigated incident as an eval case")
    ev_add.add_argument("--incident", required=True)
    ev_add.add_argument("--expected", required=True, help="correct category, e.g. CONFIG")
    ev_add.add_argument("--note", default="")
    parser.add_argument("--config", default=None)
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    configure_logging(cfg)
    if args.cmd == "brain-check":
        sys.exit(cmd_brain_check(cfg))
    if args.cmd == "eval-add":
        from incident_core.evals import add_case

        print(f"saved {add_case(cfg, args.incident, args.expected, args.note)}")
        return
    if args.cmd == "eval":
        from incident_core.evals import print_eval, run_eval

        pack = load_pack(cfg)
        result = run_eval(cfg, pack, live=args.live)
        print_eval(result)
        sys.exit(0 if result["accuracy"] == 1.0 else 1)
    pack = load_pack(cfg)
    if args.cmd == "graph":
        cmd_graph(cfg, pack, args.out, args.png)
        return
    store = make_store(cfg, pack)
    if args.cmd in ("listen", "once"):
        cmd_pull(cfg, pack, store, once=args.cmd == "once")
    elif args.cmd == "replay":
        cmd_replay(cfg, pack, store, args.ref)
    elif args.cmd == "approve":
        cmd_approve(cfg, pack, store, args.incident, args.yes)


if __name__ == "__main__":
    main()
