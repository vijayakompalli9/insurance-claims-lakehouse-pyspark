"""Bronze: land raw feeds with explicit schemas and ingestion metadata."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings
from .exceptions import SourceFileMissingError
from .logging_utils import log_kv
from .rejects import to_rejects, write_rejects
from .schemas import CORRUPT_COL, FEEDS
from .storage import write_partitions

log = logging.getLogger(__name__)


@dataclass
class BronzeResult:
    entity: str
    accepted: DataFrame
    accepted_count: int
    rejected_count: int


def read_feed(spark: SparkSession, settings: Settings, entity: str, batch_id: int) -> DataFrame:
    """Read one raw feed in PERMISSIVE mode, capturing unparseable rows."""
    file_name, fmt, schema, _ = FEEDS[entity]
    path = settings.raw_root / f"batch_{batch_id}" / file_name
    if not path.exists():
        raise SourceFileMissingError(f"missing feed {path}")
    reader = spark.read.schema(schema).option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
    if fmt == "csv":
        reader = reader.option("header", "true").option("enforceSchema", "true")
    df = reader.format(fmt).load(str(path))
    return df.select(
        "*",
        F.lit(batch_id).alias("batch_id"),
        F.col("_metadata.file_path").alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
    )


def ingest(spark: SparkSession, settings: Settings, entity: str, batch_id: int) -> BronzeResult:
    """Land ``entity`` for ``batch_id``; quarantine malformed and null-key rows."""
    _, _, schema, key = FEEDS[entity]
    business_cols = [f.name for f in schema.fields if f.name != CORRUPT_COL]
    # cache: Spark requires materialisation before filtering on the corrupt column
    raw = read_feed(spark, settings, entity, batch_id).cache()
    key_blank = F.col(key).isNull() | (F.trim(F.col(key)) == "")
    flagged = raw.withColumn(
        "reason_code",
        F.when(F.col(CORRUPT_COL).isNotNull(), F.lit("MALFORMED_RECORD"))
        .when(key_blank, F.lit("NULL_KEY")),
    )
    rejects = to_rejects(flagged.filter(F.col("reason_code").isNotNull()), entity, "bronze",
                         key, business_cols)
    accepted = flagged.filter(F.col("reason_code").isNull()) \
        .drop("reason_code", CORRUPT_COL)

    write_partitions(accepted, settings.bronze / entity, ["batch_id"])
    write_rejects(settings, rejects, entity)
    accepted_count = accepted.count()
    rejected_count = rejects.count()
    raw.unpersist()
    log_kv(log, "bronze landed", entity=entity, batch=batch_id,
           accepted=accepted_count, rejected=rejected_count)
    bronze = spark.read.parquet(str(settings.bronze / entity)) \
        .filter(F.col("batch_id") == batch_id)
    return BronzeResult(entity, bronze, accepted_count, rejected_count)
