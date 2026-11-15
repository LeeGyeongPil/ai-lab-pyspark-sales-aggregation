"""Step 6: save partitioned Parquet and read one month back (partition pruning)."""

from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from ..data import csv_dir
from ..interpret import save_sentences
from . import Context
from .explain import plan_text

CODE = """
df.write.mode("overwrite").partitionBy("month").parquet("output/sales_parquet")   # month=1 .. month=12
spark.read.parquet("output/sales_parquet").where("month = 3").explain()          # PartitionFilters
"""


_FILTER = re.compile(r"PartitionFilters: \[[^\]]*\]")


def parquet_dir(workdir: Path) -> Path:
    return workdir / "output" / "sales_parquet"


def save_parquet(df: DataFrame, path: Path) -> None:
    """Write one folder per month; the month column becomes part of the folder name."""
    df.write.mode("overwrite").partitionBy("month").parquet(str(path))


def month_dirs(path: Path) -> list[str]:
    """Names of the month=N folders, sorted by month number."""
    names = [p.name for p in path.iterdir() if p.is_dir() and p.name.startswith("month=")]
    return sorted(names, key=lambda n: int(n.split("=")[1]))


def pruning_plan(spark: SparkSession, path: Path, month: int = 3) -> str:
    """Plan text for reading a single month; it contains PartitionFilters when pruning works."""
    return plan_text(spark.read.parquet(str(path)).where(f"month = {month}"))


def folder_size(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def run(ctx: Context) -> None:
    ctx.header("save")
    ctx.code(CODE)
    df = ctx.sales()
    out = parquet_dir(ctx.workdir)
    save_parquet(df, out)
    dirs = month_dirs(out)
    back = ctx.spark.read.parquet(str(out))
    n_back, n_src = back.count(), df.count()
    print("\n" + ctx.t("save.dirs", n=len(dirs), names=", ".join(dirs)))
    print(ctx.t("save.roundtrip", back=f"{n_back:,}", src=f"{n_src:,}"))
    plan = pruning_plan(ctx.spark, out)
    lines = _FILTER.findall(plan)
    print("\n" + ctx.t("save.plan"))
    for line in lines:
        print(f"  >> FileScan parquet ... {line} ...")
    csv_size, pq_size = folder_size(csv_dir(ctx.workdir)), folder_size(out)
    print("\n" + ctx.t("save.sizes", csv=f"{csv_size / 1e6:.2f}", pq=f"{pq_size / 1e6:.2f}"))
    ctx.meaning(*save_sentences(ctx.lang, bool(lines), csv_size, pq_size))
