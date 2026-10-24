"""Investigation trace: what every agent did, when, how long it took, and why.

Each step is
  - logged to the console:   HH:MM:SS [incident] node            message (1.2s)
  - appended to actions/<incident>/trace.jsonl (machine-readable, for analysis/improvement)
  - LLM prompts + responses saved to actions/<incident>/llm/NN_<role>_<purpose>.json (when enabled)
trace.md and the report's "Investigation trace" section are rendered from trace.jsonl.

The runner handles one incident at a time, so a process-wide "current" tracer is used (gatherers run in
parallel threads; writes are locked).
"""

import json
import logging
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("df-agent.trace")

# Kinds shown in the report / trace.md (everything is kept in trace.jsonl).
REPORT_KINDS = ("start", "end", "tool", "llm", "signature", "decision", "execution", "note", "error")


class Tracer:
    def __init__(self, incident_id: str, out_dir: str, save_llm_io: bool = True):
        self.incident_id = incident_id
        self.short = incident_id[:8]
        self.dir = Path(out_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.path = self.dir / "trace.jsonl"
        self.save_llm_io = save_llm_io
        self.started = time.monotonic()
        self.llm_calls = 0
        self._lock = threading.Lock()

    def event(self, node: str, kind: str, message: str, duration_s: float = None, level=logging.INFO, **data):
        record = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "elapsed_s": round(time.monotonic() - self.started, 2),
            "node": node,
            "kind": kind,
            "message": message,
            "duration_s": round(duration_s, 2) if duration_s is not None else None,
            "data": data or None,
        }
        with self._lock:
            with self.path.open("a") as f:
                f.write(json.dumps(record, default=str) + "\n")
        suffix = f" ({duration_s:.1f}s)" if duration_s is not None else ""
        log.log(level, "[%s] %-16s %s%s", self.short, node, message, suffix)
        return record

    @contextmanager
    def span(self, node: str, message: str, **data):
        """Logs start, then end (with duration) or error."""
        self.event(node, "start", f"▶ {message}", **data)
        t0 = time.monotonic()
        result = {}
        try:
            yield result
        except Exception as exc:
            self.event(node, "error", f"✖ failed: {exc}", time.monotonic() - t0, level=logging.ERROR)
            raise
        self.event(node, "end", f"■ {result.get('summary', 'done')}", time.monotonic() - t0)

    def llm_call(self, role: str, purpose: str, model: str, system: str, user: str, output, error, duration_s: float, attempt: int):
        with self._lock:
            self.llm_calls += 1
            n = self.llm_calls
        file = None
        if self.save_llm_io:
            llm_dir = self.dir / "llm"
            llm_dir.mkdir(exist_ok=True)
            file = llm_dir / f"{n:02d}_{role}_{purpose}.json"
            file.write_text(json.dumps({"role": role, "purpose": purpose, "model": model, "attempt": attempt,
                                        "duration_s": round(duration_s, 2), "error": error, "system": system,
                                        "user": user, "output": output}, indent=2, default=str))  # fmt: skip
        status = "ok" if error is None else f"INVALID ({str(error)[:120]})"
        self.event(role, "llm", f"LLM #{n} {purpose} [{model}] attempt {attempt}: {status}", duration_s,
                   level=logging.INFO if error is None else logging.WARNING,
                   prompt_chars=len(system) + len(user), file=str(file) if file else None)  # fmt: skip

    def records(self) -> list:
        if not self.path.exists():
            return []
        return [json.loads(line) for line in self.path.read_text().splitlines() if line.strip()]


class _NullTracer(Tracer):
    def __init__(self):  # noqa: D401 - no files, console only
        self.incident_id, self.short, self.llm_calls, self.save_llm_io = "-", "-", 0, False
        self.started = time.monotonic()
        self._lock = threading.Lock()

    def event(self, node, kind, message, duration_s=None, level=logging.INFO, **data):
        log.log(level, "[-] %-16s %s", node, message)

    def llm_call(self, *args, **kwargs):
        pass

    def records(self):
        return []


_current: Tracer = _NullTracer()


def start_trace(incident_id: str, out_dir: str, save_llm_io: bool = True) -> Tracer:
    """Starts a fresh trace; a previous run's trace.jsonl / llm/ (e.g. before a replay) is kept with a timestamp suffix."""
    global _current
    out = Path(out_dir)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    for old in (out / "trace.jsonl", out / "llm"):
        if old.exists():
            old.rename(out / f"{old.stem}.{stamp}{old.suffix}")
    _current = Tracer(incident_id, out_dir, save_llm_io)
    return _current


def end_trace() -> None:
    global _current
    _current = _NullTracer()


def tracer() -> Tracer:
    return _current


def render_markdown(records: list) -> str:
    lines = ["| +s | Step | What happened | Took |", "|---|---|---|---|"]
    for r in records:
        if r["kind"] not in REPORT_KINDS:
            continue
        took = f"{r['duration_s']}s" if r.get("duration_s") is not None else ""
        message = r["message"].replace("|", "/").replace("\n", " ")[:220]
        lines.append(f"| {r['elapsed_s']} | {r['node']} | {message} | {took} |")
    return "\n".join(lines)
