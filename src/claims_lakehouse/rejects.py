"""Quarantine area: every rejected row lands here with a reason code."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import Settings
from .schemas import CORRUPT_COL
from .storage import write_partitions

REJECT_COLUMNS = ["entity", "layer", "batch_id", "reason_code", "business_key",
                  "raw_payload", "source_file", "rejected_at"]


def to_rejects(df: DataFrame, entity: str, layer: str, key_col: str,
               payload_cols: list[str]) -> DataFrame:
    """Project rows carrying a ``reason_code`` column into the rejects contract."""
    payload = F.coalesce(F.col(CORRUPT_COL), F.to_json(F.struct(*payload_cols))) \
        if CORRUPT_COL in df.columns else F.to_json(F.struct(*payload_cols))
    return df.select(
        F.lit(entity).alias("entity"),
        F.lit(layer).alias("layer"),
        F.col("batch_id").cast("int"),
        F.col("reason_code"),
        F.col(key_col).alias("business_key"),
        payload.alias("raw_payload"),
        F.col("source_file"),
        F.current_timestamp().alias("rejected_at"),
    )


def write_rejects(settings: Settings, rejects: DataFrame, entity: str) -> None:
    """Replace this (batch, layer) slice of the entity's quarantine table."""
    write_partitions(rejects.select(*REJECT_COLUMNS), settings.rejects / entity,
                     ["batch_id", "layer"])
