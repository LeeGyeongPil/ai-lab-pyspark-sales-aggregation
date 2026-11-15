"""Pure Python reference calculations over the collected records (no Spark)."""

from __future__ import annotations

import datetime as dt
from collections import defaultdict

UTC = dt.timezone.utc


def when(rec: dict) -> dt.datetime:
    return dt.datetime.fromtimestamp(rec["epoch"], tz=UTC)


def amount(rec: dict) -> int:
    return rec["qty"] * rec["unit_price"]


def monthly(records: list[dict]) -> dict[tuple[str, int], dict]:
    """(store, month) -> sum, avg, distinct product count."""
    groups: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for rec in records:
        groups[(rec["store"], when(rec).month)].append(rec)
    return {
        key: {
            "sum": sum(amount(r) for r in rows),
            "avg": sum(amount(r) for r in rows) / len(rows),
            "distinct": len({r["product"] for r in rows}),
        }
        for key, rows in groups.items()
    }


def by_category(records: list[dict]) -> dict[str, int]:
    out: dict[str, int] = defaultdict(int)
    for rec in records:
        out[rec["category"]] += amount(rec)
    return dict(out)


def rank_mom_cum(records: list[dict]) -> dict[tuple[str, int], dict]:
    """Monthly rank (1 = highest sales), % change vs previous month, running total."""
    m = monthly(records)
    out: dict[tuple[str, int], dict] = {}
    for month in range(1, 13):
        ordered = sorted((s for s, mm in m if mm == month), key=lambda s: -m[(s, month)]["sum"])
        for position, store in enumerate(ordered, start=1):
            out[(store, month)] = {"rank": position}
    for store in {s for s, _ in m}:
        running = 0
        for month in range(1, 13):
            sales = m[(store, month)]["sum"]
            running += sales
            prev = m.get((store, month - 1), {}).get("sum")
            out[(store, month)].update(
                sales=sales,
                cum=running,
                mom=None if prev is None else round((sales - prev) / prev * 100, 2),
            )
    return out


def moving_averages(records: list[dict]) -> dict[str, dict[int, tuple[float, float, bool]]]:
    """store -> day number -> (7-row average, 7-day average); plus whether the store has gaps."""
    start = dt.date(2025, 1, 1)
    daily: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for rec in records:
        daily[rec["store"]][(when(rec).date() - start).days] += amount(rec)
    out: dict[str, dict[int, tuple[float, float, bool]]] = {}
    for store, days in daily.items():
        ordered = sorted(days)
        has_gaps = len(ordered) < ordered[-1] - ordered[0] + 1
        result: dict[int, tuple[float, float, bool]] = {}
        for i, day in enumerate(ordered):
            last_rows = [days[d] for d in ordered[max(0, i - 6): i + 1]]
            last_days = [days[d] for d in ordered if day - 6 <= d <= day]
            result[day] = (sum(last_rows) / len(last_rows), sum(last_days) / len(last_days), has_gaps)
        out[store] = result
    return out
