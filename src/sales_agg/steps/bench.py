"""Benchmark: how does total time grow when the number of rows grows?"""

from __future__ import annotations

import csv
import shutil
import statistics
import time
from dataclasses import dataclass
from pathlib import Path

from pyspark import SparkContext
from pyspark.sql import SparkSession

from ..data import generate_sales, prepare
from ..i18n import t
from ..interpret import bench_sentence
from ..session import build_session
from ..table import format_table
from .aggregate import monthly_by_store
from .save import save_parquet
from .window import add_rank_mom_cum

HEADER = ["rows", "session_s", "agg_window_s", "parquet_s", "total_s"]
WARMUP_ROWS = 20_000
DEFAULT_SIZES = "100000,1000000,10000000"


@dataclass
class Timing:
    rows: int
    session_s: float
    agg_window_s: float
    parquet_s: float

    @property
    def total_s(self) -> float:
        return self.session_s + self.agg_window_s + self.parquet_s


def parse_sizes(text: str) -> list[int]:
    """Parse "100000,1000000" into [100000, 1000000]; raise ValueError with the fix."""
    try:
        sizes = [int(part.replace("_", "")) for part in text.split(",") if part.strip()]
    except ValueError as err:
        raise ValueError(f"--rows must be comma-separated integers, got {text!r}") from err
    if not sizes or any(s < 1 for s in sizes):
        raise ValueError(f"--rows needs at least one positive integer, got {text!r}")
    return sizes


def stop_spark_and_jvm() -> None:
    """Stop the session and the Java process, so the next session pays the real start-up cost."""
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
    if SparkContext._gateway is not None:  # PySpark keeps the JVM alive after stop(); end it too
        SparkContext._gateway.shutdown()
        SparkContext._gateway = None
        SparkContext._jvm = None


def measure_once(workdir: Path, rows: int, seed: int, lang: str) -> Timing:
    """One timed run: fresh session, in-memory data, aggregate + windows, Parquet write."""
    stop_spark_and_jvm()  # so that session start-up is measured every time
    t0 = time.perf_counter()
    spark = build_session(workdir, lang=lang)
    session_s = time.perf_counter() - t0

    t1 = time.perf_counter()
    df = prepare(generate_sales(spark, rows, seed))
    add_rank_mom_cum(monthly_by_store(df)).collect()  # action: runs generation + shuffle + windows
    agg_window_s = time.perf_counter() - t1

    out = workdir / "output" / "bench_parquet"
    t2 = time.perf_counter()
    save_parquet(df, out)
    parquet_s = time.perf_counter() - t2
    shutil.rmtree(out, ignore_errors=True)
    return Timing(rows, session_s, agg_window_s, parquet_s)


def run_bench(workdir: Path, sizes: list[int], repeat: int, seed: int, lang: str) -> list[Timing]:
    """Warm up once (not recorded), then time each size `repeat` times and keep the median."""
    print(t(lang, "bench.warmup", rows=f"{WARMUP_ROWS:,}"))
    results: list[Timing] = []
    try:
        measure_once(workdir, WARMUP_ROWS, seed, lang)
        for rows in sizes:
            runs = [measure_once(workdir, rows, seed, lang) for _ in range(repeat)]
            med = Timing(
                rows,
                statistics.median(r.session_s for r in runs),
                statistics.median(r.agg_window_s for r in runs),
                statistics.median(r.parquet_s for r in runs),
            )
            results.append(med)
            print(t(lang, "bench.row", rows=f"{rows:,}", total=f"{med.total_s:.2f}"))
    finally:
        stop_spark_and_jvm()  # release the JVM even if a run failed
    return results


def write_csv_report(path: Path, results: list[Timing]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(HEADER)
        for r in results:
            writer.writerow(
                [r.rows, f"{r.session_s:.2f}", f"{r.agg_window_s:.2f}", f"{r.parquet_s:.2f}",
                 f"{r.total_s:.2f}"]
            )


def run(workdir: Path, sizes: list[int], repeat: int, seed: int, lang: str) -> list[Timing]:
    """Run the benchmark, print a table and the interpretation, write output/bench.csv."""
    results = run_bench(workdir, sizes, repeat, seed, lang)
    csv_path = workdir / "output" / "bench.csv"
    write_csv_report(csv_path, results)
    print("\n" + t(lang, "bench.table"))
    print(
        format_table(
            HEADER,
            [[r.rows, round(r.session_s, 2), round(r.agg_window_s, 2), round(r.parquet_s, 2),
              round(r.total_s, 2)] for r in results],
        )
    )
    print("\n" + t(lang, "bench.saved", path=csv_path))
    print("\n" + t(lang, "label.meaning"))
    if len(results) >= 2:
        first, last = results[0], results[-1]
        print("  -> " + bench_sentence(lang, (first.rows, first.total_s), (last.rows, last.total_s)))
    print("  -> " + t(lang, "i.bench_small"))
    return results
