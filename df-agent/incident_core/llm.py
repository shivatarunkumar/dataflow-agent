"""LLM provider factory (per agent role) and a robust structured-output helper."""

import logging
import time

from langchain_core.messages import HumanMessage, SystemMessage

log = logging.getLogger(__name__)


def role_spec(cfg: dict, role: str) -> dict:
    spec = dict(cfg["llm"]["default"])
    spec.update(cfg["llm"].get("roles", {}).get(role) or {})
    return spec


def get_llm(cfg: dict, role: str):
    """Returns a LangChain chat model for the role, or None in rules-only mode (provider: none)."""
    spec = role_spec(cfg, role)
    provider = spec.get("provider", "none")
    if provider == "none":
        return None

    from langchain.chat_models import init_chat_model

    kwargs = {"temperature": spec.get("temperature", 0)}
    if provider == "ollama":
        kwargs["base_url"] = spec.get("base_url") or "http://localhost:11434"
    elif provider == "anthropic":
        kwargs["max_tokens"] = spec.get("max_tokens", 4096)
    else:
        raise ValueError(f"Unsupported LLM provider '{provider}' for role {role}")
    return init_chat_model(spec["model"], model_provider=provider, **kwargs)


def llm_label(cfg: dict, role: str) -> str:
    spec = role_spec(cfg, role)
    return f"{spec.get('provider')}:{spec.get('model', '-')}"


def model_name(llm) -> str:
    return getattr(llm, "model", None) or getattr(llm, "model_name", None) or type(llm).__name__


def structured(llm, schema, system: str, user: str, retries: int = 1, role: str = "llm", purpose: str = None):
    """Invokes llm.with_structured_output(schema); one retry with the validation error. None on failure.

    Every attempt is traced (role, purpose, model, duration, valid/invalid, prompt+response saved).
    """
    from incident_core.trace import tracer

    purpose = purpose or schema.__name__
    if llm is None:
        tracer().event(role, "llm", f"LLM skipped for {purpose} (provider: none)")
        return None
    runnable = llm.with_structured_output(schema)
    messages = [SystemMessage(content=system), HumanMessage(content=user)]
    for attempt in range(1, retries + 2):
        t0 = time.monotonic()
        try:
            out = runnable.invoke(messages)
            if isinstance(out, dict):
                out = schema.model_validate(out)
            if not isinstance(out, schema):
                raise ValueError(f"unexpected output type {type(out).__name__}")
            tracer().llm_call(role, purpose, model_name(llm), system, user, out.model_dump(), None, time.monotonic() - t0, attempt)
            return out
        except Exception as exc:  # model or validation error: tell the model and retry once
            tracer().llm_call(role, purpose, model_name(llm), system, user, None, exc, time.monotonic() - t0, attempt)
            messages = messages + [
                HumanMessage(content=f"Your previous answer was invalid ({exc}). Reply again with JSON matching the schema exactly.")
            ]
    tracer().event(role, "note", f"LLM gave no valid {purpose} after {retries + 1} attempts; falling back", level=logging.WARNING)
    return None
