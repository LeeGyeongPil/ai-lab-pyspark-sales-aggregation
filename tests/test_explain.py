"""AC-5: count shuffles (Exchange) in physical plans."""

from __future__ import annotations

from sales_agg.steps.aggregate import monthly_by_store
from sales_agg.steps.explain import count_exchanges, plan_text, sum_avg_aggregation


def test_exchange_counts(sales):
    plain = plan_text(sum_avg_aggregation(sales))
    distinct = plan_text(monthly_by_store(sales))
    assert count_exchanges(plain) == 1
    assert count_exchanges(distinct) == 2
    assert "countdistinct" in distinct.lower().replace("_", "").replace("(", "").replace(" ", "")


def test_count_exchanges_on_plain_text():
    assert count_exchanges("a\nExchange hashpartitioning(x, 8)\nExchange rangepartitioning(y)") == 1
    assert count_exchanges("no shuffle here") == 0
