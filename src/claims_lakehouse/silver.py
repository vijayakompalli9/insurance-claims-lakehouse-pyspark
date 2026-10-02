"""Silver: cleanse, standardise, conform types, dedupe and enforce references."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import Settings
from .logging_utils import log_kv
from .rejects import to_rejects, write_rejects
from .scd2 import apply_scd2
from .storage import publish, read_table, table_exists

log = logging.getLogger(__name__)
META = ["batch_id", "source_file", "ingested_at"]


@dataclass
class SilverResult:
    """Row accounting for one entity in one batch (feeds reconciliation)."""

    entity: str
    input_rows: int
    accepted: int
    rejected: int
    skipped_stale: int = 0
    rejects_by_reason: dict[str, int] = field(default_factory=dict)


def first_failure(rules: list[tuple[Column, str]]) -> Column:
    """Return the reason code of the first failing rule, else NULL."""
    expr = F.lit(None).cast("string")
    for condition, code in reversed(rules):
        expr = F.when(condition, F.lit(code)).otherwise(expr)
    return expr


def bad_parse(raw: str, parsed: str) -> Column:
    """True when a required raw value is missing or failed to parse."""
    return F.col(raw).isNull() | F.col(parsed).isNull()


def dedupe_latest(df: DataFrame, key: str, order_col: str) -> tuple[DataFrame, DataFrame]:
    """Keep the newest row per ``key``; return (survivors, superseded duplicates).

    Ties on ``order_col`` (exact resends) are broken deterministically by the
    serialised row content so reruns pick the same survivor.
    """
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc(),
                                        F.sha2(F.to_json(F.struct(*df.columns)), 256))
    ranked = df.withColumn("_rn", F.row_number().over(w))
    survivors = ranked.filter(F.col("_rn") == 1).drop("_rn")
    dupes = ranked.filter(F.col("_rn") > 1).drop("_rn") \
        .withColumn("reason_code", F.lit("DUPLICATE_KEY"))
    return survivors, dupes


def _split(flagged: DataFrame) -> tuple[DataFrame, DataFrame]:
    return (flagged.filter(F.col("reason_code").isNull()).drop("reason_code"),
            flagged.filter(F.col("reason_code").isNotNull()))


def _finish(settings: Settings, entity: str, key: str, raw_cols: list[str],
            rejects: list[DataFrame], input_rows: int, accepted: int,
            skipped_stale: int = 0) -> SilverResult:
    all_rejects = rejects[0]
    for r in rejects[1:]:
        all_rejects = all_rejects.unionByName(r, allowMissingColumns=True)
    out = to_rejects(all_rejects, entity, "silver", key, raw_cols).cache()
    write_rejects(settings, out, entity)
    by_reason = {r["reason_code"]: r["n"] for r in
                 out.groupBy("reason_code").agg(F.count("*").alias("n")).collect()}
    out.unpersist()
    result = SilverResult(entity, input_rows, accepted, sum(by_reason.values()),
                          skipped_stale, dict(sorted(by_reason.items())))
    log_kv(log, "silver conformed", entity=entity, input=input_rows, accepted=accepted,
           rejected=result.rejected, skipped_stale=skipped_stale, reasons=by_reason)
    return result


# --------------------------------------------------------------------- customers
CUSTOMER_RAW = ["customer_id", "first_name", "last_name", "date_of_birth", "email", "state",
                "segment", "updated_at"]


def process_customers(spark: SparkSession, settings: Settings, bronze: DataFrame) -> SilverResult:
    """SCD1 upsert of the customer master into ``silver/customers``."""
    typed = bronze.select(
        *CUSTOMER_RAW, *META,
        F.trim("customer_id").alias("c_id"),
        F.initcap(F.trim("first_name")).alias("c_first"),
        F.initcap(F.trim("last_name")).alias("c_last"),
        F.col("date_of_birth").try_cast("date").alias("c_dob"),
        F.lower(F.trim("email")).alias("c_email"),
        F.upper(F.trim("state")).alias("c_state"),
        F.upper(F.trim("segment")).alias("c_segment"),
        F.col("updated_at").try_cast("timestamp").alias("c_updated"),
    )
    flagged = typed.withColumn("reason_code", first_failure([
        (bad_parse("date_of_birth", "c_dob") | bad_parse("updated_at", "c_updated"),
         "INVALID_DATE"),
    ]))
    valid, invalid = _split(flagged)
    survivors, dupes = dedupe_latest(valid, "c_id", "c_updated")
    incoming = survivors.select(
        F.col("c_id").alias("customer_id"), F.col("c_first").alias("first_name"),
        F.col("c_last").alias("last_name"), F.col("c_dob").alias("date_of_birth"),
        F.col("c_email").alias("email"), F.col("c_state").alias("state"),
        F.col("c_segment").alias("segment"), F.col("c_updated").alias("updated_at"),
        "batch_id",
    )
    target = settings.silver / "customers"
    merged = incoming
    if table_exists(target):
        merged = read_table(spark, target).unionByName(incoming)
        merged, _ = dedupe_latest(merged, "customer_id", "updated_at")
    publish(merged, target)
    return _finish(settings, "customers", "customer_id", CUSTOMER_RAW, [invalid, dupes],
                   bronze.count(), incoming.count())


# --------------------------------------------------------------------- policies
POLICY_RAW = ["policy_id", "customer_id", "product_line", "premium_annual", "coverage_limit",
              "deductible", "status", "policy_start", "policy_end", "region", "updated_at"]


def process_policies(spark: SparkSession, settings: Settings, bronze: DataFrame,
                     watermark: str | None, batch_id: int) -> tuple[SilverResult, str | None]:
    """Apply new policy records after ``watermark`` to the SCD2 ``dim_policy``.

    Returns the result and the new watermark (max ``updated_at`` applied).
    """
    typed = bronze.select(
        *POLICY_RAW, *META,
        F.trim("policy_id").alias("p_id"),
        F.trim("customer_id").alias("p_customer"),
        F.upper(F.trim("product_line")).alias("p_product"),
        F.col("premium_annual").try_cast("decimal(12,2)").alias("p_premium"),
        F.col("coverage_limit").try_cast("decimal(14,2)").alias("p_limit"),
        F.col("deductible").try_cast("decimal(10,2)").alias("p_deductible"),
        F.upper(F.trim("status")).alias("p_status"),
        F.col("policy_start").try_cast("date").alias("p_start"),
        F.col("policy_end").try_cast("date").alias("p_end"),
        F.upper(F.trim("region")).alias("p_region"),
        F.col("updated_at").try_cast("timestamp").alias("p_updated"),
    )
    customers = read_table(spark, settings.silver / "customers").select(
        F.col("customer_id").alias("_cust")).distinct()
    typed = typed.join(customers, typed.p_customer == customers._cust, "left")
    flagged = typed.withColumn("reason_code", first_failure([
        (bad_parse("policy_start", "p_start") | bad_parse("policy_end", "p_end")
         | bad_parse("updated_at", "p_updated") | (F.col("p_end") < F.col("p_start")),
         "INVALID_DATE"),
        (bad_parse("premium_annual", "p_premium") | bad_parse("coverage_limit", "p_limit")
         | bad_parse("deductible", "p_deductible"), "INVALID_NUMBER"),
        ((F.col("p_premium") < 0) | (F.col("p_limit") < 0) | (F.col("p_deductible") < 0),
         "NEGATIVE_AMOUNT"),
        (F.col("_cust").isNull(), "UNKNOWN_CUSTOMER"),
    ])).drop("_cust")
    valid, invalid = _split(flagged)
    # exact resends of the same (policy_id, updated_at) are duplicates
    w = Window.partitionBy("p_id", "p_updated").orderBy(F.col("ingested_at"))
    ranked = valid.withColumn("_rn", F.row_number().over(w))
    dupes = ranked.filter("_rn > 1").drop("_rn").withColumn("reason_code", F.lit("DUPLICATE_KEY"))
    valid = ranked.filter("_rn = 1").drop("_rn").cache()

    fresh = valid if watermark is None else valid.filter(
        F.col("p_updated") > F.lit(watermark).cast("timestamp"))
    skipped = valid.count() - fresh.count()
    incoming = fresh.select(
        F.col("p_id").alias("policy_id"), F.col("p_customer").alias("customer_id"),
        F.col("p_product").alias("product_line"), F.col("p_premium").alias("premium_annual"),
        F.col("p_limit").alias("coverage_limit"), F.col("p_deductible").alias("deductible"),
        F.col("p_status").alias("status"), F.col("p_start").alias("policy_start"),
        F.col("p_end").alias("policy_end"), F.col("p_region").alias("region"),
        F.col("p_updated").alias("updated_at"),
    )
    # format inside Spark (session TZ = UTC) so Python's local TZ never shifts it
    max_seen = incoming.agg(
        F.date_format(F.max("updated_at"), "yyyy-MM-dd HH:mm:ss.SSSSSS")).first()[0]
    new_watermark = max_seen if max_seen is not None else watermark

    target = settings.silver / "dim_policy"
    current = read_table(spark, target) if table_exists(target) else None
    publish(apply_scd2(current, incoming, batch_id), target)
    accepted = incoming.count()
    valid.unpersist()
    result = _finish(settings, "policies", "policy_id", POLICY_RAW, [invalid, dupes],
                     bronze.count(), accepted, skipped)
    return result, new_watermark


# --------------------------------------------------------------------- claims
CLAIM_RAW = ["claim_id", "policy_id", "loss_date", "reported_date", "claim_amount",
             "paid_amount", "claim_status", "cause_of_loss", "last_modified"]


def process_claims(spark: SparkSession, settings: Settings, bronze: DataFrame) -> SilverResult:
    """Validate, dedupe and upsert claims into ``silver/claims`` (latest state)."""
    typed = bronze.select(
        *CLAIM_RAW, *META,
        F.trim("claim_id").alias("k_id"),
        F.trim("policy_id").alias("k_policy"),
        F.col("loss_date").try_cast("date").alias("k_loss"),
        F.col("reported_date").try_cast("date").alias("k_reported"),
        F.col("claim_amount").try_cast("decimal(14,2)").alias("k_amount"),
        F.col("paid_amount").try_cast("decimal(14,2)").alias("k_paid"),
        F.upper(F.trim("claim_status")).alias("k_status"),
        F.upper(F.trim("cause_of_loss")).alias("k_cause"),
        F.col("last_modified").try_cast("timestamp").alias("k_modified"),
    )
    policies = read_table(spark, settings.silver / "dim_policy") \
        .select(F.col("policy_id").alias("_pol")).distinct()
    typed = typed.join(policies, typed.k_policy == policies._pol, "left")
    flagged = typed.withColumn("reason_code", first_failure([
        (bad_parse("loss_date", "k_loss") | bad_parse("reported_date", "k_reported")
         | bad_parse("last_modified", "k_modified")
         | (F.col("k_reported") < F.col("k_loss")), "INVALID_DATE"),
        (bad_parse("claim_amount", "k_amount") | bad_parse("paid_amount", "k_paid"),
         "INVALID_NUMBER"),
        ((F.col("k_amount") < 0) | (F.col("k_paid") < 0), "NEGATIVE_AMOUNT"),
        (F.col("_pol").isNull(), "UNKNOWN_POLICY"),
    ])).drop("_pol")
    valid, invalid = _split(flagged)
    survivors, dupes = dedupe_latest(valid, "k_id", "k_modified")
    incoming = survivors.select(
        F.col("k_id").alias("claim_id"), F.col("k_policy").alias("policy_id"),
        F.col("k_loss").alias("loss_date"), F.col("k_reported").alias("reported_date"),
        F.col("k_amount").alias("claim_amount"), F.col("k_paid").alias("paid_amount"),
        F.col("k_status").alias("claim_status"), F.col("k_cause").alias("cause_of_loss"),
        F.col("k_modified").alias("last_modified"), "batch_id",
    )
    target = settings.silver / "claims"
    merged = incoming
    if table_exists(target):
        merged = read_table(spark, target).unionByName(incoming)
        merged, _ = dedupe_latest(merged, "claim_id", "last_modified")
    publish(merged, target)
    return _finish(settings, "claims", "claim_id", CLAIM_RAW, [invalid, dupes],
                   bronze.count(), incoming.count())
