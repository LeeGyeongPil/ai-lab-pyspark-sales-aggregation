"""AC-7: session settings, sandbox-safe folders and the time zone trap."""

from __future__ import annotations

import os

from pyspark.sql import functions as F

from sales_agg.session import TMP_DIR_NAME, prepare_environment


def test_session_settings_and_timezone_trap(spark, raw, workdir):
    conf = spark.conf
    assert conf.get("spark.sql.session.timeZone") == "UTC"
    assert conf.get("spark.sql.shuffle.partitions") == "8"
    tmp = str((workdir / TMP_DIR_NAME).resolve())
    assert os.environ["SPARK_LOCAL_DIRS"] == tmp
    jvm_tmp = spark.sparkContext._jvm.java.lang.System.getProperty("java.io.tmpdir")
    assert jvm_tmp == tmp

    df = raw.withColumn("amount", F.col("qty") * F.col("unit_price"))

    def january_total() -> int:
        # 연-월로 거른다: 월 숫자만 쓰면 2026년 1월 1일(서울)이 섞인다
        return df.where(F.date_format("ts", "yyyy-MM") == "2025-01").agg(F.sum("amount")).first()[0]

    utc_total = january_total()
    conf.set("spark.sql.session.timeZone", "Asia/Seoul")
    try:
        seoul_total = january_total()
    finally:
        conf.set("spark.sql.session.timeZone", "UTC")
    # rows between 15:00 and 24:00 UTC on Jan 31 are February in Seoul: January only shrinks
    jan31_evening = df.where(
        (F.col("ts") >= F.lit("2025-01-31 15:00:00").cast("timestamp"))
        & (F.col("ts") < F.lit("2025-02-01 00:00:00").cast("timestamp"))
    ).agg(F.sum("amount")).first()[0]
    assert jan31_evening > 0
    assert seoul_total == utc_total - jan31_evening
    assert january_total() == utc_total


def test_prepare_environment_respects_existing_values(monkeypatch, tmp_path):
    monkeypatch.setenv("JAVA_TOOL_OPTIONS", "-Djava.io.tmpdir=/custom/tmp")
    monkeypatch.setenv("SPARK_LOCAL_DIRS", "/custom/spark")
    prepare_environment(tmp_path)
    assert os.environ["JAVA_TOOL_OPTIONS"] == "-Djava.io.tmpdir=/custom/tmp"
    assert os.environ["SPARK_LOCAL_DIRS"] == "/custom/spark"
    prepare_environment(tmp_path, force=True)
    assert str(tmp_path.resolve() / TMP_DIR_NAME) in os.environ["JAVA_TOOL_OPTIONS"]
    assert "/custom/tmp" not in os.environ["JAVA_TOOL_OPTIONS"]
    assert os.environ["SPARK_LOCAL_DIRS"] == str(tmp_path.resolve() / TMP_DIR_NAME)
