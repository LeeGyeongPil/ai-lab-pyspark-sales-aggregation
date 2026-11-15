"""Turn computed numbers into plain-language sentences ("what it means").

Every sentence is filled from the real result, and says honestly that the pattern
comes from the synthetic data generator.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence

from .i18n import t


def aggregate_sentences(lang: str, monthly: Sequence[tuple[str, int, int]]) -> list[str]:
    """monthly = (store, month, sales) rows."""
    by_store: dict[str, int] = defaultdict(int)
    by_month: dict[int, int] = defaultdict(int)
    for store, month, sales in monthly:
        by_store[store] += sales
        by_month[month] += sales
    total = sum(by_store.values())
    top = max(by_store, key=lambda s: by_store[s])
    low = min(by_store, key=lambda s: by_store[s])
    best = max(by_month, key=lambda m: by_month[m])
    worst = min(by_month, key=lambda m: by_month[m])
    return [
        t(lang, "i.stores", top=top, top_total=f"{by_store[top]:,}",
          share=f"{by_store[top] / total * 100:.1f}", low=low, low_total=f"{by_store[low]:,}"),
        t(lang, "i.season", best=best, best_total=f"{by_month[best]:,}",
          worst=worst, worst_total=f"{by_month[worst]:,}"),
    ]


def window_sentences(
    lang: str,
    rows: Sequence[tuple[str, int, int, int, float | None, int]],
    diff_by_store: dict[str, int],
) -> list[str]:
    """rows = (store, month, sales, rank, mom_pct, cum_sales); diff_by_store = days where rows != range."""
    out: list[str] = []
    with_mom = [r for r in rows if r[4] is not None]
    if with_mom:
        store, month, _, _, pct, _ = max(with_mom, key=lambda r: r[4])
        out.append(t(lang, "i.mom", store=store, month=month, pct=f"{pct:.1f}"))
    last_month = max(r[1] for r in rows)
    final = [r for r in rows if r[1] == last_month]
    leader = max(final, key=lambda r: r[5])
    wins = sum(1 for r in rows if r[3] == 1 and r[0] == leader[0])
    out.append(t(lang, "i.cum", store=leader[0], cum=f"{leader[5]:,}", wins=wins, months=last_month))
    if diff_by_store:
        names = ", ".join(f"{s} ({n})" for s, n in sorted(diff_by_store.items()))
        out.append(t(lang, "i.rows_range", stores=names))
    else:
        out.append(t(lang, "i.rows_range_none"))
    return out


def explain_sentences(lang: str, exchanges_plain: int, exchanges_distinct: int) -> list[str]:
    return [t(lang, "i.explain", a=exchanges_plain, b=exchanges_distinct)]


def save_sentences(lang: str, pruned: bool, csv_bytes: int, parquet_bytes: int) -> list[str]:
    prune = t(lang, "i.prune" if pruned else "i.prune_missing")
    if parquet_bytes < csv_bytes:
        return [prune, t(lang, "i.size", ratio=f"{csv_bytes / parquet_bytes if parquet_bytes else 0.0:.1f}")]
    # Parquet is as large or larger (small data): say so instead of "smaller".
    ratio = parquet_bytes / csv_bytes if csv_bytes else 0.0
    return [prune, t(lang, "i.size_larger", ratio=f"{ratio:.1f}")]


def bench_sentence(lang: str, first: tuple[int, float], last: tuple[int, float]) -> str:
    """first/last = (rows, total_seconds) of the smallest and largest benchmark size."""
    rows_x = last[0] / first[0] if first[0] else 0.0
    time_x = last[1] / first[1] if first[1] else 0.0
    return t(lang, "i.bench", rows_x=f"{rows_x:.0f}", time_x=f"{time_x:.1f}")
