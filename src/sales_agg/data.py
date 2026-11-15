"""Synthetic sales records, generated inside the JVM (no Python UDFs, no downloads).

Schema and rules are documented in docs/data-schema.md. Patterns planted on purpose:
  * year-end seasonality (November/December up, February down)
  * store S5 is closed on Mondays (no rows) -> rowsBetween and rangeBetween differ
Every key uses its own hash column, so store/product/day are independent
(`id % 5` and `id % 365` style keys would be correlated - a classic trap).
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

YEAR_START = dt.date(2025, 1, 1)
EPOCH_YEAR_START = 1735689600  # 2025-01-01 00:00:00 UTC in seconds
DAYS = 365
STORES = ["S1", "S2", "S3", "S4", "S5"]
# cumulative percentage thresholds: S1 30%, S2 25%, S3 20%, S4 15%, S5 10%
_STORE_CUTS = [30, 55, 75, 90]
CLOSED_STORE = "S5"
CLOSED_DAY_MOD = 5  # day offset % 7 == 5 is a Monday (2025-01-06)
CATEGORIES = ["Food", "Drinks", "Home", "Toys"]
# (product, category index, fixed unit price)
PRODUCTS = [
    ("P01", 0, 3200), ("P02", 0, 4500), ("P03", 0, 5800),
    ("P04", 1, 2100), ("P05", 1, 2900), ("P06", 1, 6400),
    ("P07", 2, 8900), ("P08", 2, 12500), ("P09", 2, 15800),
    ("P10", 3, 7300), ("P11", 3, 9600), ("P12", 3, 21000),
]
# demand multiplier per month (index 0 = January)
MONTH_FACTORS = [1.0, 0.8, 1.0, 1.0, 1.1, 1.1, 1.0, 1.0, 1.1, 1.2, 1.4, 1.7]

SALES_SCHEMA = StructType(
    [
        StructField("order_id", LongType(), False),
        StructField("ts", TimestampType(), False),
        StructField("store", StringType(), False),
        StructField("product", StringType(), False),
        StructField("category", StringType(), False),
        StructField("qty", IntegerType(), False),
        StructField("unit_price", IntegerType(), False),
    ]
)
_TS_FORMAT = "yyyy-MM-dd HH:mm:ss"


def _month_of_day() -> list[int]:
    return [(YEAR_START + dt.timedelta(days=d)).month for d in range(DAYS)]


def _lookup(values: list, index: Column) -> Column:
    """values[index] as a column expression (index is 0-based)."""
    return F.element_at(F.array(*[F.lit(v) for v in values]), index + 1)


def generate_sales(spark: SparkSession, rows: int, seed: int) -> DataFrame:
    """Build the sales DataFrame lazily; nothing is computed until an action runs."""
    if rows < 1:
        raise ValueError("rows must be at least 1")

    def pick(name: str, n: int) -> Column:
        # independent hash per key: same (id, seed, name) -> same value, different name -> unrelated
        return F.pmod(F.xxhash64(F.col("id"), F.lit(seed), F.lit(name)), F.lit(n))

    day = pick("day", DAYS).cast("int")
    pct = pick("store", 100)
    store = F.when(pct < _STORE_CUTS[0], "S1")
    for name, cut in zip(STORES[1:4], _STORE_CUTS[1:]):
        store = store.when(pct < cut, name)
    store = store.otherwise("S5")
    pidx = pick("product", len(PRODUCTS)).cast("int")
    factor = _lookup(MONTH_FACTORS, _lookup(_month_of_day(), day) - 1)
    base_qty = pick("qty", 5) + 1
    qty = F.greatest(F.lit(1), F.round(base_qty * factor)).cast("int")
    seconds = F.lit(EPOCH_YEAR_START) + day.cast("long") * 86400 + pick("sec", 86400)

    df = spark.range(rows).select(
        F.col("id").alias("order_id"),
        F.timestamp_seconds(seconds).alias("ts"),
        store.alias("store"),
        F.format_string("P%02d", pidx + 1).alias("product"),
        _lookup([CATEGORIES[c] for _, c, _ in PRODUCTS], pidx).alias("category"),
        qty.alias("qty"),
        _lookup([p for _, _, p in PRODUCTS], pidx).cast("int").alias("unit_price"),
        day.alias("_day"),
    )
    # the closed store has no rows on Mondays
    closed = (F.col("store") == CLOSED_STORE) & (F.col("_day") % 7 == CLOSED_DAY_MOD)
    return df.where(~closed).drop("_day")


def prepare(df: DataFrame) -> DataFrame:
    """Add the derived columns every step uses (column expressions = recipes, not values)."""
    return (
        df.withColumn("amount", (F.col("qty") * F.col("unit_price")).cast("long"))
        .withColumn("month", F.month("ts"))
        .withColumn("day", F.to_date("ts"))
        .withColumn("day_no", F.datediff("day", F.lit(YEAR_START.isoformat()).cast("date")))
    )


def csv_dir(workdir: Path) -> Path:
    return workdir / "data" / "sales_csv"


def meta_path(workdir: Path) -> Path:
    return workdir / "data" / "sales_meta.json"


def write_csv(df: DataFrame, path: Path) -> None:
    """Save as CSV (header + explicit timestamp format) so the read step starts from a file."""
    df.write.mode("overwrite").option("header", True).option("timestampFormat", _TS_FORMAT).csv(
        str(path)
    )


def read_sales_csv(spark: SparkSession, path: Path) -> DataFrame:
    """Read with an explicit schema: faster and safer than inferSchema (no extra pass)."""
    return (
        spark.read.option("header", True)
        .option("timestampFormat", _TS_FORMAT)
        .schema(SALES_SCHEMA)
        .csv(str(path))
    )


@dataclass
class DataInfo:
    rows_requested: int
    seed: int
    rows_actual: int
    reused: bool
    path: Path


def saved_meta(workdir: Path) -> dict | None:
    """Rows/seed of the CSV already on disk, or None when there is none."""
    path, meta = csv_dir(workdir), meta_path(workdir)
    if path.exists() and meta.exists():
        return json.loads(meta.read_text(encoding="utf-8"))
    return None


def ensure_csv(spark: SparkSession, workdir: Path, rows: int, seed: int, force: bool = False) -> DataInfo:
    """Generate data/sales_csv unless a file for the same rows and seed already exists."""
    path, meta = csv_dir(workdir), meta_path(workdir)
    if not force and path.exists() and meta.exists():
        saved = json.loads(meta.read_text(encoding="utf-8"))
        if saved.get("rows") == rows and saved.get("seed") == seed:
            return DataInfo(rows, seed, saved["rows_actual"], True, path)
    write_csv(generate_sales(spark, rows, seed), path)
    actual = read_sales_csv(spark, path).count()
    meta.parent.mkdir(parents=True, exist_ok=True)
    meta.write_text(
        json.dumps({"rows": rows, "seed": seed, "rows_actual": actual}), encoding="utf-8"
    )
    return DataInfo(rows, seed, actual, False, path)
