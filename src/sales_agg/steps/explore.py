"""Step 2: read the CSV, look at the schema, build columns, see lazy evaluation."""

from __future__ import annotations

import time

from pyspark.sql import functions as F

from . import Context

CODE = """
df = spark.read.option("header", True).schema(SALES_SCHEMA).csv("data/sales_csv")
df.printSchema()
big = (df.filter(F.col("qty") >= 3)                       # transformation: only a plan
         .select("store", "product", "qty", "unit_price")
         .withColumn("amount", F.col("qty") * F.col("unit_price")))
big.count()                                               # action: the work really happens
"""
PANDAS = """
df = pd.read_csv("data/sales_csv/part-*.csv")             # pandas runs every line right away
big = df[df["qty"] >= 3][["store", "product", "qty", "unit_price"]]
big["amount"] = big["qty"] * big["unit_price"]            # like an Excel formula =C2*D2
"""


def run(ctx: Context) -> None:
    ctx.header("explore")
    ctx.code(CODE)
    df = ctx.sales().select("order_id", "ts", "store", "product", "category", "qty", "unit_price")
    print(f"\n{ctx.t('explore.schema')}")
    for line in df.schema.treeString().splitlines():
        print(f"    {line}")

    t0 = time.perf_counter()
    big = (
        df.filter(F.col("qty") >= 3)
        .select("store", "product", "qty", "unit_price")
        .withColumn("amount", F.col("qty") * F.col("unit_price"))
    )
    plan_s = time.perf_counter() - t0
    t1 = time.perf_counter()
    n_big = big.count()
    act_s = time.perf_counter() - t1
    print("\n" + ctx.t("explore.lazy", plan_s=f"{plan_s:.4f}", act_s=f"{act_s:.2f}", n=f"{n_big:,}"))

    sample = big.dropDuplicates(["store", "product"]).orderBy(F.col("amount").desc(), "store", "product").limit(5).collect()
    ctx.table(
        ctx.t("explore.sample"),
        ["store", "product", "qty", "unit_price", "amount"],
        [[r.store, r["product"], r.qty, r.unit_price, r.amount] for r in sample],
    )
    print(f"\n{ctx.t('explore.pandas')}")
    for line in PANDAS.strip("\n").splitlines():
        print(f"    {line}")
    ctx.meaning(ctx.t("i.lazy", plan_s=f"{plan_s:.4f}", act_s=f"{act_s:.2f}"))
