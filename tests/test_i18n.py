"""AC-9: language precedence, identical key sets, and translated output."""

from __future__ import annotations

import pytest

from sales_agg.cli import main
from sales_agg.i18n import load_messages, resolve_lang, t


def test_lang_precedence_and_same_keys():
    en, ko = load_messages("en"), load_messages("ko")
    assert set(en) == set(ko)
    assert all(v.strip() for v in en.values()) and all(v.strip() for v in ko.values())
    # placeholders must match between the two languages
    import string

    fields = lambda s: {f[1] for f in string.Formatter().parse(s) if f[1]}
    for key in en:
        assert fields(en[key]) - {"folder_word", "exist"} == fields(ko[key]), key  # English-only plural helpers

    # --lang wins over everything
    env = {"SALES_AGG_LANG": "ko", "LANG": "ko_KR.UTF-8"}
    assert resolve_lang("en", env) == "en"
    # then SALES_AGG_LANG
    assert resolve_lang(None, {"SALES_AGG_LANG": "ko", "LANG": "en_US.UTF-8"}) == "ko"
    assert resolve_lang(None, {"SALES_AGG_LANG": "en", "LANG": "ko_KR.UTF-8"}) == "en"
    # then LC_ALL > LC_MESSAGES > LANG (first non-empty decides)
    assert resolve_lang(None, {"LC_ALL": "ko_KR.UTF-8", "LANG": "en_US.UTF-8"}) == "ko"
    assert resolve_lang(None, {"LC_ALL": "en_US.UTF-8", "LC_MESSAGES": "ko_KR.UTF-8"}) == "en"
    assert resolve_lang(None, {"LC_MESSAGES": "ko_KR.UTF-8", "LANG": "en_US.UTF-8"}) == "ko"
    assert resolve_lang(None, {"LANG": "ko_KR.UTF-8"}) == "ko"
    # anything else is English
    assert resolve_lang(None, {"LANG": "fr_FR.UTF-8"}) == "en"
    assert resolve_lang(None, {}) == "en"
    assert resolve_lang(None, {"SALES_AGG_LANG": "xx", "LANG": "ko_KR.UTF-8"}) == "ko"
    with pytest.raises(ValueError, match="--lang en"):
        resolve_lang("fr", {})


def test_help_switches_language(capsys, monkeypatch):
    monkeypatch.delenv("SALES_AGG_LANG", raising=False)
    for lang, needle in (("ko", "판매 기록"), ("en", "sales records")):
        with pytest.raises(SystemExit) as exc:
            main(["--lang", lang, "--help"])
        assert exc.value.code == 0
        assert needle in capsys.readouterr().out
    with pytest.raises(SystemExit):
        main(["generate", "--lang", "ko", "--help"])
    assert "판매 기록" in capsys.readouterr().out


def test_env_var_selects_language_for_help(capsys, monkeypatch):
    monkeypatch.setenv("SALES_AGG_LANG", "ko")
    with pytest.raises(SystemExit):
        main(["--help"])
    assert "판매 기록" in capsys.readouterr().out


def test_unsupported_lang_is_an_error(capsys):
    assert main(["--lang", "fr", "doctor"]) == 2
    assert "--lang en" in capsys.readouterr().err


def test_message_lookup_fills_placeholders():
    assert "21" in t("en", "doctor.java_ok", major=21, version="21.0.10", path="/x")
    assert "21" in t("ko", "doctor.java_ok", major=21, version="21.0.10", path="/x")
