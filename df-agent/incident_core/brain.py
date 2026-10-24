"""Domain knowledge loader: system overview, runbooks (retrieved by key), playbooks, policies, signatures."""

from pathlib import Path

import yaml


class Brain:
    def __init__(self, brain_dir: str):
        self.dir = Path(brain_dir)
        self.system = self._text("system.md")
        self.glossary = self._text("glossary.md")
        self.playbooks = self._yaml("playbooks.yaml")["playbooks"]
        self.policies = self._yaml("policies.yaml")
        self.signatures = self._yaml("signatures.yaml")["signatures"]

    def _text(self, name: str) -> str:
        p = self.dir / name
        return p.read_text() if p.exists() else ""

    def _yaml(self, name: str) -> dict:
        return yaml.safe_load((self.dir / name).read_text())

    def runbook_keys(self) -> list:
        return sorted(p.stem for p in (self.dir / "runbooks").glob("*.md"))

    def runbook(self, key: str) -> str:
        p = self.dir / "runbooks" / f"{key}.md"
        return p.read_text() if p.exists() else ""

    def runbooks(self, keys: list, max_chars: int = 6000) -> str:
        """Concatenates the requested runbooks (deduplicated, falls back to UNKNOWN), capped in size."""
        texts, seen = [], set()
        for key in keys + ["UNKNOWN"]:
            if key in seen:
                continue
            seen.add(key)
            text = self.runbook(key)
            if text and (key != "UNKNOWN" or not texts):
                texts.append(text)
        return "\n\n---\n\n".join(texts)[:max_chars]
