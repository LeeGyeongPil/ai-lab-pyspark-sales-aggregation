"""AC-1: doctor checks Java 17+ and gives guidance."""

from __future__ import annotations

import subprocess

import pytest

from sales_agg import doctor
from sales_agg.cli import main


def _fake_run(stderr: str):
    def run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr=stderr)

    return run


def test_parse_java_version_handles_old_and_new_formats():
    assert doctor.parse_java_version('openjdk version "21.0.10" 2026-01-20') == ("21.0.10", 21)
    assert doctor.parse_java_version('java version "1.8.0_392"') == ("1.8.0_392", 8)
    assert doctor.parse_java_version('openjdk version "17" 2021-09-14') == ("17", 17)
    assert doctor.parse_java_version("nothing useful") is None


def test_java_missing_or_old_gives_guidance(monkeypatch, capsys, tmp_path):
    monkeypatch.delenv("JAVA_HOME", raising=False)

    # 1) no java at all -> exit 2 with install guidance
    monkeypatch.setattr(doctor.shutil, "which", lambda name: None)
    code = main(["--lang", "en", "doctor", "--workdir", str(tmp_path)])
    out = capsys.readouterr().out
    assert code == 2
    assert "Java was not found" in out and "Install it with" in out

    # 2) Java 8 -> exit 2 and says which version was found
    monkeypatch.setattr(doctor.shutil, "which", lambda name: "/usr/bin/java")
    monkeypatch.setattr(doctor.subprocess, "run", _fake_run('java version "1.8.0_392"'))
    code = main(["--lang", "en", "doctor", "--workdir", str(tmp_path)])
    out = capsys.readouterr().out
    assert code == 2
    assert "Java 8 found" in out and "needs Java 17" in out

    # 3) Java 21 -> OK, exit 0
    monkeypatch.setattr(doctor.subprocess, "run", _fake_run('openjdk version "21.0.10" 2026-01-20'))
    code = main(["--lang", "en", "doctor", "--workdir", str(tmp_path)])
    out = capsys.readouterr().out
    assert code == 0
    assert "Java 21 found" in out and "OK" in out


def test_unwritable_temp_folder_is_reported(monkeypatch, capsys, tmp_path):
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.setattr(doctor.shutil, "which", lambda name: "/usr/bin/java")
    monkeypatch.setattr(doctor.subprocess, "run", _fake_run('openjdk version "21.0.10"'))
    blocker = tmp_path / "file-not-folder"
    blocker.write_text("x")
    code = main(["--lang", "en", "doctor", "--workdir", str(blocker)])
    assert code == 2
    assert "Cannot write to the temp folder" in capsys.readouterr().out


def test_bad_java_home_is_reported(monkeypatch, capsys, tmp_path):
    monkeypatch.setenv("JAVA_HOME", str(tmp_path / "nowhere"))
    monkeypatch.setattr(doctor.shutil, "which", lambda name: "/usr/bin/java")
    monkeypatch.setattr(doctor.subprocess, "run", _fake_run('openjdk version "21.0.10"'))
    assert main(["--lang", "en", "doctor", "--workdir", str(tmp_path)]) == 2
    assert "JAVA_HOME points to" in capsys.readouterr().out


@pytest.mark.parametrize("lang,needle", [("en", "Java was not found"), ("ko", "Java 를 찾지 못했습니다")])
def test_guidance_is_localised(monkeypatch, capsys, tmp_path, lang, needle):
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.setattr(doctor.shutil, "which", lambda name: None)
    assert main(["--lang", lang, "doctor", "--workdir", str(tmp_path)]) == 2
    assert needle in capsys.readouterr().out
