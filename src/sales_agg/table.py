"""Print small result tables with fixed-width columns."""

from __future__ import annotations

import unicodedata
from collections.abc import Sequence


def fmt(value: object) -> str:
    """Format one cell: thousands separators for numbers, two decimals for floats."""
    if value is None:
        return "-"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def width(text: str) -> int:
    """Columns the text takes in a terminal: Korean (wide) characters take two."""
    return sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1 for ch in text)


def pad(text: str, size: int, right: bool) -> str:
    """ljust/rjust by display width instead of character count."""
    space = " " * max(0, size - width(text))
    return space + text if right else text + space


def format_table(headers: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    """Return a text table; numbers are right-aligned, text left-aligned."""
    cells = [[fmt(v) for v in row] for row in rows]
    widths = [width(h) for h in headers]
    for row in cells:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], width(cell))
    numeric = [
        bool(rows) and all(isinstance(r[i], (int, float)) or r[i] is None for r in rows)
        for i in range(len(headers))
    ]

    def line(values: Sequence[str]) -> str:
        parts = [pad(v, widths[i], right=numeric[i]) for i, v in enumerate(values)]
        return "  " + "  ".join(parts).rstrip()

    out = [line(list(headers)), "  " + "  ".join("-" * w for w in widths)]
    out.extend(line(row) for row in cells)
    return "\n".join(out)
