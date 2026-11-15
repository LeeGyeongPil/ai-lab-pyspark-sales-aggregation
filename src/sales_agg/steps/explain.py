"""Step 5: read the physical plan and count shuffles ("Exchange")."""

from __future__ import annotations

import contextlib
import io

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..interpret import explain_sentences
from . import Context
from .aggregate import monthly_by_store

CODE = """
df.groupBy("store", "month").agg(F.sum("amount"), F.avg("amount")).explain()          # no action runs
df.groupBy("store", "month").agg(F.sum("amount"), F.countDistinct("product")).explain()
"""
EXCHANGE = "Exchange hashpartitioning"


def sum_avg_aggregation(df: DataFrame) -> DataFrame:
    """Aggregation that needs one shuffle: partial sums per partition, then merge by key."""
    return df.groupBy("store", "month").agg(
        F.sum("amount").alias("sales"), F.avg("amount").alias("avg_ticket")
    )


def plan_text(df: DataFrame) -> str:
    """The text `df.explain()` prints (physical plan only; nothing is computed)."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain()
    return buffer.getvalue()


def count_exchanges(plan: str) -> int:
    """Number of hash shuffles in a plan string."""
    return plan.count(EXCHANGE)


def _show_plan(ctx: Context, caption: str, plan: str) -> None:
    print(f"\n{caption}")
    for line in plan.splitlines():
        if not line.strip():
            continue
        marker = ">>" if EXCHANGE in line else "  "
        shown = line if len(line) <= 150 else line[:147] + "..."
        print(f"  {marker} {shown}")


def run(ctx: Context) -> None:
    ctx.header("explain")
    ctx.code(CODE)
    df = ctx.sales()
    plan_a = plan_text(sum_avg_aggregation(df))
    plan_b = plan_text(monthly_by_store(df))  # sum + avg + countDistinct
    n_a, n_b = count_exchanges(plan_a), count_exchanges(plan_b)
    _show_plan(ctx, ctx.t("explain.plan_a", n=n_a), plan_a)
    _show_plan(ctx, ctx.t("explain.plan_b", n=n_b), plan_b)
    ctx.meaning(*explain_sentences(ctx.lang, n_a, n_b))
