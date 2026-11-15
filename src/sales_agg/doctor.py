"""`sales-agg doctor`: check Java 17+ and a writable temp folder before Spark starts."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from .i18n import t
from .session import TMP_DIR_NAME

MIN_JAVA = 17
_VERSION = re.compile(r'version "(?P<full>(?P<a>\d+)(?:\.(?P<b>\d+))?[^"]*)"')


@dataclass
class JavaInfo:
    """What `java -version` told us."""

    path: str | None
    version: str | None = None
    major: int | None = None


def parse_java_version(text: str) -> tuple[str, int] | None:
    """Parse `java -version` output; "1.8.0_392" is Java 8, "21.0.10" is Java 21."""
    match = _VERSION.search(text)
    if not match:
        return None
    first = int(match["a"])
    major = int(match["b"] or 0) if first == 1 else first
    return match["full"], major


def find_java(env: Mapping[str, str], which: Callable[[str], str | None] | None = None) -> str | None:
    """Locate the java executable: JAVA_HOME/bin/java first, then PATH."""
    home = env.get("JAVA_HOME")
    if home:
        candidate = Path(home) / "bin" / "java"
        if candidate.exists():
            return str(candidate)
    return (which or shutil.which)("java")


def check_java(env: Mapping[str, str] | None = None) -> JavaInfo:
    """Run `java -version` and report path, version and major version."""
    env = os.environ if env is None else env
    path = find_java(env)
    if path is None:
        return JavaInfo(path=None)
    try:
        proc = subprocess.run(
            [path, "-version"], capture_output=True, text=True, timeout=30, check=False
        )
    except (OSError, subprocess.TimeoutExpired):
        return JavaInfo(path=path)
    parsed = parse_java_version(f"{proc.stderr}\n{proc.stdout}")
    if parsed is None:
        return JavaInfo(path=path)
    return JavaInfo(path=path, version=parsed[0], major=parsed[1])


def check_tmp(workdir: Path) -> tuple[bool, str]:
    """Try to create and delete a file in <workdir>/.spark-tmp."""
    tmp = workdir / TMP_DIR_NAME
    try:
        tmp.mkdir(parents=True, exist_ok=True)
        probe = tmp / ".doctor-probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
    except OSError as err:
        return False, f"{tmp}: {err}"
    return True, str(tmp)


def install_hint(lang: str) -> str:
    """OS specific Java install command."""
    key = "doctor.install_mac" if sys.platform == "darwin" else "doctor.install_linux"
    return t(lang, key, minimum=MIN_JAVA)


def run_doctor(lang: str, workdir: Path) -> int:
    """Print the checks. Exit code 0 if everything is fine, 2 otherwise."""
    ok = True
    java = check_java()
    if java.path is None:
        ok = False
        print(t(lang, "doctor.java_missing", minimum=MIN_JAVA))
        print(install_hint(lang))
    elif java.major is None:
        ok = False
        print(t(lang, "doctor.java_unknown", path=java.path))
        print(install_hint(lang))
    elif java.major < MIN_JAVA:
        ok = False
        print(t(lang, "doctor.java_old", major=java.major, version=java.version, minimum=MIN_JAVA))
        print(install_hint(lang))
    else:
        print(t(lang, "doctor.java_ok", major=java.major, version=java.version, path=java.path))

    java_home = os.environ.get("JAVA_HOME")
    if java_home and not (Path(java_home) / "bin" / "java").exists():
        ok = False
        print(t(lang, "doctor.java_home_bad", path=java_home))

    tmp_ok, detail = check_tmp(workdir)
    if tmp_ok:
        print(t(lang, "doctor.tmp_ok", path=detail))
    else:
        ok = False
        print(t(lang, "doctor.tmp_fail", detail=detail))

    print(t(lang, "doctor.ok" if ok else "doctor.fail"))
    return 0 if ok else 2
