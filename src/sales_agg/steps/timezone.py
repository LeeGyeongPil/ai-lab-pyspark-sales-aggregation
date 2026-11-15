"""Extra step: the time zone trap. Same stored instants, different month totals."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..data import EPOCH_YEAR_START
from . import Context

SESSION_TZ = "UTC"
OTHER_TZ = "Asia/Seoul"
TARGET_MONTH = "2025-01"  # 연-월로 고른다: 월 숫자만 쓰면 다른 해의 1월이 섞인다

HOUR = 3600
DAY = 86400
# 서울(UTC+9)에서 날짜가 하루 넘어가는 UTC 시각대 (epoch 초, 시간대와 무관한 값)
JAN31_EVENING = (EPOCH_YEAR_START + 30 * DAY + 15 * HOUR, EPOCH_YEAR_START + 31 * DAY)
DEC31_EVENING = (EPOCH_YEAR_START + 364 * DAY + 15 * HOUR, EPOCH_YEAR_START + 365 * DAY)

CODE = """
df = read_sales_csv(...).cache()                  # parse the CSV under UTC and keep the instants
df.count()                                        # materialize them now
spark.conf.set("spark.sql.session.timeZone", "Asia/Seoul")
ym = F.date_format("ts", "yyyy-MM")               # year-month, cut at Seoul midnight
df.where(ym == "2025-01").agg(F.sum("amount"))    # same instants, different January
"""


@dataclass
class TzResult:
    store: str
    utc_total: int  # 2025-01 total when the session zone is UTC
    other_total: int  # 2025-01 total when the session zone is Asia/Seoul
    left_rows: int  # rows of Jan 31 15:00-24:00 UTC (Feb 1 in Seoul)
    left_amount: int
    month_only_total: int  # Seoul total if you group by month number only (year ignored)
    joined_amount: int  # Dec 31 15:00-24:00 UTC, which is Jan 1 2026 in Seoul


def month_total(df: DataFrame, store: str, year_month: str | None = TARGET_MONTH) -> int:
    """Total of one store for a year-month in the current session zone.

    year_month=None reproduces the second trap: filtering on the month number only.
    """
    one = df.where(F.col("store") == store)
    if year_month is None:
        one = one.where(F.month("ts") == 1)
    else:
        one = one.where(F.date_format("ts", "yyyy-MM") == year_month)
    return int(one.agg(F.sum("amount")).first()[0] or 0)


def window_sum(df: DataFrame, store: str, start: int, end: int) -> tuple[int, int]:
    """(row count, amount) of one store between two epoch seconds — independent of time zone."""
    epoch = F.col("ts").cast("long")
    row = (
        df.where((F.col("store") == store) & (epoch >= start) & (epoch < end))
        .agg(F.count("*"), F.sum("amount"))
        .first()
    )
    return int(row[0]), int(row[1] or 0)


def compare(ctx: Context, tz: str = OTHER_TZ) -> TzResult:
    """Compute the January 2025 total of the first store under UTC and under tz."""
    spark = ctx.spark
    df = ctx.sales().cache()
    df.count()  # fill the cache while the session zone is still UTC
    store = df.select("store").distinct().orderBy("store").first()["store"]
    left_rows, left_amount = window_sum(df, store, *JAN31_EVENING)
    _, joined_amount = window_sum(df, store, *DEC31_EVENING)
    spark.conf.set("spark.sql.session.timeZone", SESSION_TZ)
    base = month_total(df, store)
    spark.conf.set("spark.sql.session.timeZone", tz)
    try:
        other = month_total(df, store)
        month_only = month_total(df, store, year_month=None)
    finally:
        spark.conf.set("spark.sql.session.timeZone", SESSION_TZ)
        df.unpersist()
    return TzResult(store, base, other, left_rows, left_amount, month_only, joined_amount)


def run(ctx: Context) -> None:
    print()
    print(ctx.t("tz.title"))
    print(ctx.t("tz.what"))
    ctx.code(CODE)
    r = compare(ctx)
    ctx.table(
        ctx.t("tz.table", store=r.store, month=TARGET_MONTH),
        [ctx.t("tz.col.zone"), ctx.t("tz.col.total")],
        [[SESSION_TZ, f"{r.utc_total:,}"], [OTHER_TZ, f"{r.other_total:,}"]],
    )
    ctx.meaning(
        ctx.t(
            "i.tz",
            diff=f"{r.other_total - r.utc_total:+,}",
            rows=f"{r.left_rows:,}",
            amount=f"{r.left_amount:,}",
        ),
        ctx.t(
            "i.tz_year",
            total=f"{r.month_only_total:,}",
            amount=f"{r.joined_amount:,}",
        ),
        ctx.t("i.tz_csv"),
    )
