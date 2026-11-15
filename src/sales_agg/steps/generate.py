"""Step 1: generate synthetic sales records and save them as CSV."""

from __future__ import annotations

from ..data import ensure_csv
from . import Context

CODE = """
spark.range(rows)                                   # ids 0..rows-1, spread over partitions
    .select(F.timestamp_seconds(...).alias("ts"),   # every column is a hash expression
            store_expr.alias("store"), ...)         # -> computed inside the JVM, no Python loop
    .write.option("header", True).csv("data/sales_csv")
"""


def run(ctx: Context) -> None:
    ctx.header("generate")
    ctx.code(CODE)
    info = ensure_csv(ctx.spark, ctx.workdir, ctx.rows, ctx.seed)
    key = "generate.reused" if info.reused else "generate.done"
    print(
        "\n"
        + ctx.t(key, requested=f"{info.rows_requested:,}", actual=f"{info.rows_actual:,}",
                seed=info.seed, path=info.path)
    )
    ctx.meaning(ctx.t("i.generate", closed=f"{info.rows_requested - info.rows_actual:,}"))
