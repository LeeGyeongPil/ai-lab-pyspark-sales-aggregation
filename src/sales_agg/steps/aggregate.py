"""Step 3: sum / avg / countDistinct by store, month and category, plus a pivot table."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..interpret import aggregate_sentences
from . import Context

CODE = """
monthly = (df.groupBy("store", "month")
             .agg(F.sum("amount").alias("sales"),
                  F.avg("amount").alias("avg_ticket"),
                  F.countDistinct("product").alias("n_products")))
pivot = df.groupBy("store").pivot("month", list(range(1, 13))).sum("amount")  # like an Excel pivot
"""
MONTHS = list(range(1, 13))


def monthly_by_store(df: DataFrame) -> DataFrame:
    """One row per store and month: total sales, average ticket, distinct products sold."""
    return df.groupBy("store", "month").agg(
        F.sum("amount").alias("sales"),
        F.avg("amount").alias("avg_ticket"),
        F.countDistinct("product").alias("n_products"),
    )


def by_category(df: DataFrame) -> DataFrame:
    """Total sales and order lines per category."""
    return df.groupBy("category").agg(F.sum("amount").alias("sales"), F.count("*").alias("lines"))


def store_month_pivot(df: DataFrame) -> DataFrame:
    """Store x month table; giving the month list avoids an extra pass over the data."""
    return df.groupBy("store").pivot("month", MONTHS).sum("amount")


def run(ctx: Context) -> None:
    ctx.header("aggregate")
    ctx.code(CODE)
    df = ctx.sales()
    monthly = monthly_by_store(df).orderBy("store", "month").collect()
    ctx.table(
        ctx.t("aggregate.t_monthly"),
        ["store", "month", "sales", "avg_ticket", "n_products"],
        [[r.store, r.month, r.sales, float(r.avg_ticket), r.n_products] for r in monthly[:6]],
    )
    cats = by_category(df).orderBy(F.desc("sales")).collect()
    ctx.table(
        ctx.t("aggregate.t_category"),
        ["category", "sales", "lines"],
        [[r.category, r.sales, r.lines] for r in cats],
    )
    pivot = store_month_pivot(df).orderBy("store").collect()
    ctx.table(
        ctx.t("aggregate.t_pivot"),
        ["store"] + [f"m{m}" for m in MONTHS],
        [[r.store] + [r[str(m)] for m in MONTHS] for r in pivot],
    )
    ctx.meaning(*aggregate_sentences(ctx.lang, [(r.store, r.month, r.sales) for r in monthly]))
