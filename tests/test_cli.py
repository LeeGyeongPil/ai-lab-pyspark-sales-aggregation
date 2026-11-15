"""End-to-end CLI checks on small data, in both languages (AC-8, AC-9, AC-10).

Spark-running commands go through a subprocess: the CLI stops its session when it ends,
which would kill the shared in-process session used by the other tests.
"""

from __future__ import annotations

import csv
import os
import subprocess
import sys

import pytest

from sales_agg.cli import main
from sales_agg.steps.bench import parse_sizes
from sales_agg.table import format_table

TITLES_EN = ["[1/6] Generate", "[2/6] Explore", "[3/6] Aggregate", "[4/6] Window", "[5/6] Explain", "[6/6] Save"]


def run_cli(*args: str) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if k not in ("SALES_AGG_LANG", "SPARK_LOCAL_DIRS")}
    return subprocess.run(
        [sys.executable, "-m", "sales_agg.cli", *args],
        capture_output=True, text=True, timeout=500, env=env, check=False,
    )


def test_tour_runs_all_steps_in_english(tmp_path):
    proc = run_cli("--lang", "en", "tour", "--rows", "3000", "--seed", "1", "--workdir", str(tmp_path))
    assert proc.returncode == 0, proc.stderr
    out = proc.stdout
    positions = [out.index(title) for title in TITLES_EN]
    assert positions == sorted(positions)
    assert "What it means:" in out
    assert (tmp_path / "output" / "sales_parquet" / "month=12").is_dir()
    # a single step afterwards reuses the CSV written by the tour
    again = run_cli("--lang", "en", "generate", "--rows", "3000", "--seed", "1", "--workdir", str(tmp_path))
    assert again.returncode == 0, again.stderr
    assert "Reusing existing data" in again.stdout


def test_tour_in_korean_uses_korean_titles(tmp_path):
    proc = run_cli("--lang", "ko", "tour", "--rows", "2000", "--workdir", str(tmp_path))
    assert proc.returncode == 0, proc.stderr
    assert "[4/6] 윈도 (Window)" in proc.stdout and "그래서 무슨 뜻인가:" in proc.stdout
    assert "What it means:" not in proc.stdout


def test_bench_writes_csv_and_interpretation(tmp_path):
    proc = run_cli("--lang", "en", "bench", "--rows", "2000,8000", "--repeat", "1", "--workdir", str(tmp_path))
    assert proc.returncode == 0, proc.stderr
    with (tmp_path / "output" / "bench.csv").open() as fh:
        rows = list(csv.reader(fh))
    assert rows[0] == ["rows", "session_s", "agg_window_s", "parquet_s", "total_s"]
    assert [r[0] for r in rows[1:]] == ["2000", "8000"]
    assert "Warm-up run" in proc.stdout and "Rows grew 4x" in proc.stdout


def test_bad_inputs_give_fix_hints(tmp_path, capsys):
    assert main(["--lang", "en", "bench", "--rows", "abc", "--workdir", str(tmp_path)]) == 2
    assert "comma-separated integers" in capsys.readouterr().err
    assert main(["--lang", "en", "bench", "--rows", "100", "--repeat", "0", "--workdir", str(tmp_path)]) == 2
    assert "--repeat must be at least 1" in capsys.readouterr().err
    assert main(["--lang", "en", "explore", "--rows", "0", "--workdir", str(tmp_path)]) == 2
    assert "--rows must be at least 1" in capsys.readouterr().err


def test_parse_sizes():
    assert parse_sizes("100000, 1_000_000") == [100000, 1000000]
    with pytest.raises(ValueError):
        parse_sizes("0")
    with pytest.raises(ValueError):
        parse_sizes(",")


def test_format_table_alignment():
    text = format_table(["name", "n"], [["a", 1234], ["bb", None]])
    assert "1,234" in text and text.splitlines()[1].startswith("  ----")
    # 한국어 머리글은 글자당 두 칸: 구분선 길이가 화면 폭과 맞아야 한다
    ko = format_table(["세션 시간대", "합계"], [["UTC", 10]]).splitlines()
    assert ko[1] == "  -----------  ----"
    assert ko[2] == "  UTC            10"


def test_timezone_command_shows_month_shift(tmp_path):
    proc = run_cli("--lang", "en", "timezone", "--rows", "20000", "--seed", "7", "--workdir", str(tmp_path))
    assert proc.returncode == 0, proc.stderr
    assert "Time zone trap" in proc.stdout
    lines = [ln for ln in proc.stdout.splitlines() if ln.strip().startswith(("UTC", "Asia/Seoul"))]
    totals = [int(ln.split()[-1].replace(",", "")) for ln in lines]
    assert len(totals) == 2
    # 연-월 기준이면 서울 쪽 1월은 1월 31일 저녁 몫만큼 줄어든다
    assert totals[1] < totals[0]
    assert f"{totals[1] - totals[0]:+,}" in proc.stdout
    assert "Second trap" in proc.stdout


def test_other_rows_prints_regenerate_notice(tmp_path):
    first = run_cli("--lang", "en", "generate", "--rows", "2000", "--seed", "3", "--workdir", str(tmp_path))
    assert first.returncode == 0, first.stderr
    second = run_cli("--lang", "en", "explore", "--rows", "1500", "--seed", "3", "--workdir", str(tmp_path))
    assert second.returncode == 0, second.stderr
    assert "is regenerated" in second.stdout and "--rows 2000" in second.stdout
