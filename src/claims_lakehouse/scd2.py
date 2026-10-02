"""SCD Type 2 merge for ``dim_policy`` using hash-diff change detection.

Validity intervals are half-open: ``effective_from <= t < effective_to``.
The open version carries ``effective_to = 9999-12-31`` and ``is_current = true``.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

TRACKED_COLUMNS = ["customer_id", "product_line", "premium_annual", "coverage_limit",
                   "deductible", "status", "policy_start", "policy_end", "region"]
OPEN_END = "9999-12-31 00:00:00"
DIM_COLUMNS = ["policy_sk", "policy_id", *TRACKED_COLUMNS, "hash_diff", "source_updated_at",
               "effective_from", "effective_to", "is_current", "batch_id"]


def hash_diff(columns: list[str]) -> Column:
    """SHA-256 over the tracked attributes; nulls get a sentinel so they compare."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in columns]
    return F.sha2(F.concat_ws("||", *parts), 256)


def apply_scd2(current: DataFrame | None, incoming: DataFrame, batch_id: int) -> DataFrame:
    """Merge ``incoming`` policy records into the existing dimension.

    ``incoming`` must contain ``policy_id``, the tracked columns and
    ``updated_at`` (timestamp). Several changes to one policy in the same batch
    are applied in ``updated_at`` order. Records whose hash matches the version
    before them are ignored, so replays are idempotent.
    """
    inc = incoming.select(
        "policy_id", *TRACKED_COLUMNS,
        hash_diff(TRACKED_COLUMNS).alias("hash_diff"),
        F.col("updated_at").alias("source_updated_at"),
        F.lit(None).cast("timestamp").alias("effective_from"),
        F.lit(batch_id).alias("batch_id"),
        F.lit(False).alias("is_existing"),
    )
    if current is None:
        history = None
        candidates = inc
    else:
        history = current.filter(~F.col("is_current")).select(*DIM_COLUMNS)
        open_rows = current.filter(F.col("is_current")).select(
            "policy_id", *TRACKED_COLUMNS, "hash_diff", "source_updated_at",
            "effective_from", "batch_id", F.lit(True).alias("is_existing"),
        )
        # per-key guard: never replay a record at or before the open version
        latest = open_rows.select("policy_id", F.col("source_updated_at").alias("_open_ts"))
        inc = inc.join(latest, "policy_id", "left").filter(
            F.col("_open_ts").isNull() | (F.col("source_updated_at") > F.col("_open_ts"))
        ).drop("_open_ts")
        candidates = open_rows.unionByName(inc)

    order = Window.partitionBy("policy_id").orderBy(
        F.col("source_updated_at"), F.col("is_existing").desc())
    changed = candidates.withColumn("prev_hash", F.lag("hash_diff").over(order)).filter(
        F.col("is_existing") | F.col("prev_hash").isNull()
        | (F.col("prev_hash") != F.col("hash_diff"))
    )
    first_version = F.row_number().over(order) == 1
    start_ts = F.least(F.col("policy_start").cast("timestamp"), F.col("source_updated_at"))
    versions = changed.withColumn(
        "effective_from",
        F.when(F.col("is_existing"), F.col("effective_from"))
        .when(first_version, start_ts)
        .otherwise(F.col("source_updated_at")),
    )
    next_from = F.lead("effective_from").over(order)
    versions = versions.withColumn("next_from", next_from).select(
        F.xxhash64("policy_id", "effective_from").alias("policy_sk"),
        "policy_id", *TRACKED_COLUMNS, "hash_diff", "source_updated_at", "effective_from",
        F.coalesce(F.col("next_from"), F.lit(OPEN_END).cast("timestamp")).alias("effective_to"),
        F.col("next_from").isNull().alias("is_current"),
        F.col("batch_id").cast("int").alias("batch_id"),
    )
    return versions if history is None else history.unionByName(versions)
