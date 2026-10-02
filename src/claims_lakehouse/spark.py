"""SparkSession factory tuned for a small local lakehouse run."""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark(master: str = "local[2]", shuffle_partitions: int = 4,
                app_name: str = "claims-lakehouse") -> SparkSession:
    """Create (or reuse) a local SparkSession with deterministic settings."""
    spark = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
