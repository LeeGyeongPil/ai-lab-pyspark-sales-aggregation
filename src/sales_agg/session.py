"""Create the SparkSession with the settings this series standardises on.

* spark.sql.session.timeZone = UTC     -> month/day boundaries do not depend on your computer
* spark.sql.shuffle.partitions = 8     -> the default 200 only creates empty tasks on a laptop
* java.io.tmpdir and SPARK_LOCAL_DIRS  -> <workdir>/.spark-tmp, so locked-down machines work
* UI off unless asked                  -> no port 4040 needed
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from pyspark.errors import PySparkRuntimeError
from pyspark.sql import SparkSession

from .i18n import t

TMP_DIR_NAME = ".spark-tmp"
_TMPDIR_OPTION = re.compile(r'-Djava\.io\.tmpdir=("[^"]*"|\S+)\s*')


class SessionError(RuntimeError):
    """Raised when Spark cannot start; the message already contains the fix."""


def prepare_environment(workdir: Path, force: bool = False) -> Path:
    """Point the JVM temp folder and Spark scratch folder into <workdir>/.spark-tmp.

    Values you already set are respected unless force=True.
    """
    tmp = (workdir / TMP_DIR_NAME).resolve()
    tmp.mkdir(parents=True, exist_ok=True)
    opts = os.environ.get("JAVA_TOOL_OPTIONS", "")
    if force:
        opts = _TMPDIR_OPTION.sub("", opts).strip()
        os.environ.pop("SPARK_LOCAL_DIRS", None)
    if "java.io.tmpdir" not in opts:
        value = f'"{tmp}"' if " " in str(tmp) else str(tmp)
        opts = f"{opts} -Djava.io.tmpdir={value}".strip()
    os.environ["JAVA_TOOL_OPTIONS"] = opts
    os.environ.setdefault("SPARK_LOCAL_DIRS", str(tmp))
    return tmp


def build_session(
    workdir: Path, ui: bool = False, shuffle: int = 8, lang: str = "en", force_env: bool = False
) -> SparkSession:
    """Start (or reuse) a local[*] session. Raises SessionError with a fix on failure."""
    prepare_environment(workdir, force=force_env)
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("sales-agg")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle))
        .config("spark.ui.enabled", "true" if ui else "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )
    try:
        spark = builder.getOrCreate()
    except PySparkRuntimeError as err:
        if "JAVA_GATEWAY_EXITED" in str(err):
            raise SessionError(t(lang, "error.gateway")) from err
        raise
    spark.sparkContext.setLogLevel("ERROR")
    return spark
