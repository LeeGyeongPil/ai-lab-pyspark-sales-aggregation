"""Screen language selection (en/ko) and message lookup.

Precedence: --lang > SALES_AGG_LANG > LC_ALL / LC_MESSAGES / LANG.
A locale that starts with "ko" selects Korean; anything else selects English.
All user-facing text lives in messages/en.json and messages/ko.json.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from functools import lru_cache
from importlib import resources

SUPPORTED = ("en", "ko")
DEFAULT = "en"
ENV_VAR = "SALES_AGG_LANG"


def normalize(value: str | None) -> str | None:
    """Return "en" or "ko" for a language/locale string, or None if it is empty or unknown."""
    if not value:
        return None
    short = value.strip().lower().replace("-", "_").split("_")[0].split(".")[0]
    return short if short in SUPPORTED else None


def resolve_lang(cli_lang: str | None, env: Mapping[str, str]) -> str:
    """Pick the screen language following the documented precedence."""
    if cli_lang is not None:
        lang = normalize(cli_lang)
        if lang is None:
            raise ValueError(
                f"Unsupported language {cli_lang!r}. Use --lang en or --lang ko."
            )
        return lang
    from_env = normalize(env.get(ENV_VAR))
    if from_env:
        return from_env
    for name in ("LC_ALL", "LC_MESSAGES", "LANG"):
        locale = env.get(name)
        if locale:  # the first non-empty variable decides (POSIX rule)
            return "ko" if locale.lower().startswith("ko") else DEFAULT
    return DEFAULT


@lru_cache(maxsize=None)
def load_messages(lang: str) -> dict[str, str]:
    """Load the message table for one language."""
    text = resources.files("sales_agg").joinpath("messages", f"{lang}.json").read_text("utf-8")
    return json.loads(text)


def t(lang: str, key: str, **values: object) -> str:
    """Look up a message and fill its {placeholders}."""
    return load_messages(lang)[key].format(**values)
