"""Step implementations. Each step module has pure compute functions plus run(ctx)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from ..data import csv_dir, ensure_csv, prepare, read_sales_csv, saved_meta
from ..i18n import t
from ..table import format_table


@dataclass
class Context:
    """Everything a step needs: session, folders, data size and screen language."""

    spark: SparkSession
    workdir: Path
    rows: int
    seed: int
    lang: str
    step_no: int = 0
    total_steps: int = 6

    def t(self, key: str, **values: object) -> str:
        return t(self.lang, key, **values)

    def header(self, name: str) -> None:
        """Print the numbered step title and its one-line description."""
        title = self.t(f"step.title.{name}")
        print()
        print(self.t("step.header", n=self.step_no, total=self.total_steps, title=title))
        print(self.t(f"step.what.{name}"))

    def code(self, snippet: str) -> None:
        print(f"\n{self.t('label.code')}")
        for line in snippet.strip("\n").splitlines():
            print(f"    {line}")

    def table(self, caption: str, headers: list[str], rows: list[list[object]]) -> None:
        print(f"\n{caption}")
        print(format_table(headers, rows))

    def meaning(self, *sentences: str) -> None:
        print(f"\n{self.t('label.meaning')}")
        for sentence in sentences:
            print(f"  -> {sentence}")

    def sales(self) -> DataFrame:
        """Prepared sales DataFrame read from CSV (generated first if needed)."""
        saved = saved_meta(self.workdir)
        if saved and (saved.get("rows"), saved.get("seed")) != (self.rows, self.seed):
            print(
                self.t("notice.regenerate", saved_rows=f"{saved.get('rows')}",
                       saved_seed=saved.get("seed"), rows=self.rows, seed=self.seed)
            )
        ensure_csv(self.spark, self.workdir, self.rows, self.seed)
        return prepare(read_sales_csv(self.spark, csv_dir(self.workdir)))
