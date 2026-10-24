import os
import re
from pathlib import Path

import yaml

AGENT_ROOT = Path(__file__).resolve().parent.parent
_VAR = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


def _expand(value):
    if isinstance(value, str):
        return _VAR.sub(lambda m: os.environ.get(m.group(1)) or (m.group(2) or ""), value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


def load_config(path=None) -> dict:
    """Loads config.yaml with ${VAR:-default} expansion; relative dirs resolve against df-agent/."""
    cfg = _expand(yaml.safe_load(Path(path or AGENT_ROOT / "config.yaml").read_text()))
    for key in ("brain_dir", "actions_dir", "state_dir"):
        p = Path(cfg[key])
        cfg[key] = str(p if p.is_absolute() else AGENT_ROOT / p)
    return cfg
