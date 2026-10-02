"""Table read/write helpers. Single place to swap Parquet for Delta Lake.

Tables are directories of Parquet files. ``publish`` writes to a staging
directory first and then swaps it into place, so a failed write or a blocked
quality gate never leaves a half-written table visible to consumers.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

FORMAT = "parquet"


def table_exists(path: Path) -> bool:
    return path.exists() and any(path.glob("**/*.parquet"))


def read_table(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.format(FORMAT).load(str(path))


def write_partitions(df: DataFrame, path: Path, partition_cols: list[str]) -> None:
    """Append-or-replace the partitions present in ``df`` (dynamic overwrite)."""
    df.write.format(FORMAT).mode("overwrite").partitionBy(*partition_cols).save(str(path))


def stage(df: DataFrame, path: Path) -> Path:
    """Fully materialise ``df`` into ``<path>.__staging`` and return that path."""
    staging = path.with_name(path.name + ".__staging")
    if staging.exists():
        shutil.rmtree(staging)
    df.write.format(FORMAT).mode("overwrite").save(str(staging))
    return staging


def promote(staging: Path, path: Path) -> None:
    """Swap a staged table into its final location."""
    if path.exists():
        shutil.rmtree(path)
    staging.rename(path)


def publish(df: DataFrame, path: Path) -> None:
    """Stage then promote. Safe even when ``df`` was derived from ``path``."""
    promote(stage(df, path), path)


def discard(staging: Path) -> None:
    if staging.exists():
        shutil.rmtree(staging)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n")
