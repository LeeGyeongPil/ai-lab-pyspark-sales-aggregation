"""AC-2: the generator is deterministic and its keys are independent."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from sales_agg.data import generate_sales, prepare, read_sales_csv, write_csv

from .reference import when


def _fingerprint(df):
    row = df.agg(
        F.count("*").alias("n"), F.sum(F.col("qty").cast("long") * F.col("unit_price")).alias("s")
    ).first()
    return row.n, row.s


def test_generate_deterministic_and_independent(spark, raw, records):
    same_seed = generate_sales(spark, 20_000, 7)
    other_seed = generate_sales(spark, 20_000, 8)
    assert _fingerprint(raw) == _fingerprint(same_seed)
    assert _fingerprint(raw)[1] != _fingerprint(other_seed)[1]

    # store S5 is closed on Mondays: no row on a Monday (weekday() == 0)
    s5 = [r for r in records if r["store"] == "S5"]
    assert s5, "S5 should still sell on other days"
    assert all(when(r).weekday() != 0 for r in s5)
    # other stores do sell on Mondays
    assert any(when(r).weekday() == 0 for r in records if r["store"] == "S1")

    # independent keys: each open store appears on many different days
    # (with `id % 5` and `id % 365` keys, store 0 would only appear every 5 days)
    for store in ("S1", "S2", "S3", "S4"):
        days = {when(r).date() for r in records if r["store"] == store}
        assert len(days) >= 300, f"{store} appears on only {len(days)} days"
    assert len({r["product"] for r in records if r["store"] == "S4"}) == 12


def test_rejects_non_positive_rows(spark):
    with pytest.raises(ValueError, match="rows must be at least 1"):
        generate_sales(spark, 0, 1)


def test_csv_roundtrip_keeps_rows_and_timestamps(spark, raw, workdir):
    path = workdir / "csv_roundtrip"
    write_csv(raw, path)
    back = read_sales_csv(spark, path)
    assert back.count() == raw.count()
    assert _fingerprint(back) == _fingerprint(raw)
    # timestamps survive the CSV round trip (compared as epoch seconds)
    a = raw.agg(F.sum(F.col("ts").cast("long"))).first()[0]
    b = back.agg(F.sum(F.col("ts").cast("long"))).first()[0]
    assert a == b


def test_prepare_adds_derived_columns(sales):
    cols = set(sales.columns)
    assert {"amount", "month", "day", "day_no"} <= cols
    row = sales.first()
    assert row.amount == row.qty * row.unit_price
