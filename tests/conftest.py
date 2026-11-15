"""Shared fixtures: one real local Spark session (starting the JVM is slow) and fixed data."""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession

from sales_agg.data import generate_sales, prepare
from sales_agg.session import build_session

TEST_ROWS = 20_000  # big enough that every open store sells on (almost) every day
TEST_SEED = 7


@pytest.fixture(scope="session")
def workdir(tmp_path_factory: pytest.TempPathFactory):
    return tmp_path_factory.mktemp("sales-agg-work")


@pytest.fixture(scope="session")
def spark(workdir) -> SparkSession:
    # force_env: tests must not depend on JAVA_TOOL_OPTIONS / SPARK_LOCAL_DIRS of the machine
    session = build_session(workdir, ui=False, force_env=True)
    yield session
    session.stop()


@pytest.fixture(scope="session")
def raw(spark) -> DataFrame:
    return generate_sales(spark, TEST_ROWS, TEST_SEED)


@pytest.fixture(scope="session")
def sales(raw) -> DataFrame:
    return prepare(raw)


@pytest.fixture(scope="session")
def records(raw) -> list[dict]:
    """Plain Python copy of the data; timestamps as epoch seconds (no time zone surprises)."""
    rows = raw.selectExpr(
        "store", "product", "category", "qty", "unit_price", "cast(ts as long) as epoch"
    ).collect()
    return [r.asDict() for r in rows]
