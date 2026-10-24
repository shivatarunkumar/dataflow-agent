import sys
from pathlib import Path

import pytest

AGENT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AGENT_ROOT))

from incident_core.config import load_config  # noqa: E402


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    """Rules-only config (no LLM), outputs in a temp dir."""
    monkeypatch.setenv("DFA_LANDING_BUCKET", "test-landing")
    config = load_config()
    config["llm"] = {"default": {"provider": "none"}, "roles": {}}
    config["actions_dir"] = str(tmp_path / "actions")
    config["state_dir"] = str(tmp_path / "state")
    return config
