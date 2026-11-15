"""AC-6: partitioned Parquet round trip and partition pruning."""

from __future__ import annotations

from sales_agg.steps.save import month_dirs, pruning_plan, save_parquet


def test_partitioned_parquet_roundtrip(spark, sales, workdir):
    out = workdir / "parquet_roundtrip"
    save_parquet(sales, out)
    assert month_dirs(out) == [f"month={m}" for m in range(1, 13)]
    assert spark.read.parquet(str(out)).count() == sales.count()
    plan = pruning_plan(spark, out, month=3)
    assert "PartitionFilters: [isnotnull(month" in plan
    assert "= 3)" in plan
    # reading the whole dataset has no partition filter
    assert "PartitionFilters: []" in _plan(spark, out)


def _plan(spark, out):
    from sales_agg.steps.explain import plan_text

    return plan_text(spark.read.parquet(str(out)))
