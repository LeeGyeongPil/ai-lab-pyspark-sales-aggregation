"""AC-3: aggregations equal a pure Python reference."""

from __future__ import annotations

from sales_agg.steps.aggregate import by_category, monthly_by_store, store_month_pivot

from . import reference


def test_aggregates_match_reference(sales, records):
    expected = reference.monthly(records)
    rows = monthly_by_store(sales).collect()
    assert len(rows) == len(expected) == 60
    for r in rows:
        exp = expected[(r.store, r.month)]
        assert r.sales == exp["sum"]
        assert round(float(r.avg_ticket), 2) == round(exp["avg"], 2)
        assert r.n_products == exp["distinct"]

    cats = {r.category: r.sales for r in by_category(sales).collect()}
    assert cats == reference.by_category(records)

    pivot = store_month_pivot(sales).collect()
    assert [c for c in store_month_pivot(sales).columns] == ["store"] + [str(m) for m in range(1, 13)]
    for r in pivot:
        for month in range(1, 13):
            assert r[str(month)] == expected[(r.store, month)]["sum"]
