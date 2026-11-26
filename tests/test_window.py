"""AC-4: rank, month-over-month, running total and rows-vs-range moving averages."""

from __future__ import annotations

from sales_agg.steps.aggregate import monthly_by_store
from sales_agg.steps.window import add_rank_mom_cum, moving_avg_rows_vs_range

from . import reference


def test_rank_mom_cum_and_rows_vs_range(sales, records):
    expected = reference.rank_mom_cum(records)
    rows = add_rank_mom_cum(monthly_by_store(sales)).collect()
    assert len(rows) == 60
    for r in rows:
        exp = expected[(r.store, r.month)]
        assert r.rank == exp["rank"]
        assert r.cum_sales == exp["cum"]
        if exp["mom"] is None:
            assert r.mom_pct is None  # January has no previous month
        else:
            assert abs(r.mom_pct - exp["mom"]) < 0.011

    exp_ma = reference.moving_averages(records)
    got = {(r.store, r.day_no): r for r in moving_avg_rows_vs_range(sales).collect()}
    differing_days: dict[str, int] = {}
    for store, days in exp_ma.items():
        for day, (rows_avg, range_avg, has_gaps) in days.items():
            r = got[(store, day)]
            assert abs(r.ma7_rows - rows_avg) < 1e-6
            assert abs(r.ma7_range - range_avg) < 1e-6
            if abs(r.ma7_rows - r.ma7_range) > 1e-6:
                differing_days[store] = differing_days.get(store, 0) + 1
        has_gaps = next(iter(days.values()))[2]
        if has_gaps:
            assert differing_days.get(store, 0) >= 1, f"{store} has gaps but rows == range"
        else:
            assert differing_days.get(store, 0) == 0, f"{store} has no gaps but rows != range"

    assert exp_ma["S5"][next(iter(exp_ma["S5"]))][2] is True  # S5 really has closed days
    assert "S5" in differing_days
    # at least three stores have no closed days, so the equality half of the test is meaningful
    assert sum(1 for s in exp_ma if s not in differing_days) >= 3


def test_mom_is_null_when_previous_month_is_missing(spark):
    """A store with sales only in months 2 and 4 must not compare month 4 with month 2."""
    monthly = spark.createDataFrame(
        [("S1", 2, 100), ("S1", 4, 150), ("S1", 5, 300)], "store string, month int, sales long"
    )
    rows = {r.month: r for r in add_rank_mom_cum(monthly).collect()}
    assert rows[2].mom_pct is None
    assert rows[4].mom_pct is None and rows[4].prev is None  # month 3 is missing
    assert rows[5].mom_pct == 100.0  # consecutive months still work
    assert rows[5].cum_sales == 550
