"""Step 4: window functions - rank, previous month, running total, moving averages."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ..interpret import window_sentences
from . import Context
from .aggregate import monthly_by_store

CODE = """
by_store = Window.partitionBy("store").orderBy("month")
by_month = Window.partitionBy("month").orderBy(F.desc("sales"))
res = (monthly.withColumn("rank", F.rank().over(by_month))                    # 1 = best store
              .withColumn("prev", F.when(F.lag("month").over(by_store) == F.col("month") - 1,
                                         F.lag("sales").over(by_store)))      # last month (null if missing)
              .withColumn("mom_pct", F.round((F.col("sales") - F.col("prev")) / F.col("prev") * 100, 2))
              .withColumn("cum_sales", F.sum("sales").over(
                  by_store.rowsBetween(Window.unboundedPreceding, 0))))       # running total
w = Window.partitionBy("store").orderBy("day_no")
daily.withColumn("ma7_rows",  F.avg("daily_sales").over(w.rowsBetween(-6, 0)))   # last 7 ROWS
daily.withColumn("ma7_range", F.avg("daily_sales").over(w.rangeBetween(-6, 0)))  # last 7 DAYS
"""


def add_rank_mom_cum(monthly: DataFrame) -> DataFrame:
    """Add monthly rank (1 = highest sales), previous month, change in % and running total."""
    by_store = Window.partitionBy("store").orderBy("month")
    by_month = Window.partitionBy("month").orderBy(F.desc("sales"))
    return (
        monthly.withColumn("rank", F.rank().over(by_month))
        .withColumn(
            "prev",
            # only compare with the row before when it really is the previous calendar month
            F.when(
                F.lag("month").over(by_store) == F.col("month") - 1,
                F.lag("sales").over(by_store),
            ),
        )
        .withColumn("mom_pct", F.round((F.col("sales") - F.col("prev")) / F.col("prev") * 100, 2))
        .withColumn(
            "cum_sales", F.sum("sales").over(by_store.rowsBetween(Window.unboundedPreceding, 0))
        )
    )


def moving_avg_rows_vs_range(df: DataFrame) -> DataFrame:
    """7-row vs 7-day moving average of daily sales. They differ when a store has days without rows."""
    daily = df.groupBy("store", "day_no", "day").agg(F.sum("amount").alias("daily_sales"))
    w = Window.partitionBy("store").orderBy("day_no")
    return (
        daily.withColumn("ma7_rows", F.avg("daily_sales").over(w.rowsBetween(-6, 0)))
        .withColumn("ma7_range", F.avg("daily_sales").over(w.rangeBetween(-6, 0)))
    )


def run(ctx: Context) -> None:
    ctx.header("window")
    ctx.code(CODE)
    df = ctx.sales()
    res = add_rank_mom_cum(monthly_by_store(df)).orderBy("store", "month").collect()
    shown = [r for r in res if r.store == "S2"]
    ctx.table(
        ctx.t("window.t_rank", store="S2"),
        ["month", "sales", "rank", "mom_pct", "cum_sales"],
        [[r.month, r.sales, r.rank, r.mom_pct, r.cum_sales] for r in shown],
    )
    ma = moving_avg_rows_vs_range(df).orderBy("store", "day_no").collect()
    diff = [r for r in ma if abs(r.ma7_rows - r.ma7_range) > 1e-9]
    by_store: dict[str, int] = {}
    for r in diff:
        by_store[r.store] = by_store.get(r.store, 0) + 1
    first = [r for r in diff if r.store == "S5"][:6]
    ctx.table(
        ctx.t("window.t_ma", store="S5"),
        ["day", "daily_sales", "ma7_rows", "ma7_range"],
        [[str(r.day), r.daily_sales, float(r.ma7_rows), float(r.ma7_range)] for r in first],
    )
    ctx.meaning(
        *window_sentences(
            ctx.lang,
            [(r.store, r.month, r.sales, r.rank, r.mom_pct, r.cum_sales) for r in res],
            by_store,
        )
    )
