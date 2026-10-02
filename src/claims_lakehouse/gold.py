"""Gold: star schema (fact_claims + conformed dimensions) and KPI mart.

Gold is fully derived from silver on every run, so it is idempotent. Tables
are staged first and promoted only after the data-quality gate passes.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings
from .storage import promote, read_table, stage

GOLD_TABLES = ["dim_date", "dim_customer", "dim_policy", "fact_claims", "kpi_loss_ratio_monthly"]


def build_dim_customer(customers: DataFrame) -> DataFrame:
    """BI-safe customer dimension: direct identifiers are hashed or dropped."""
    age = F.floor(F.months_between(F.lit("2026-01-01").cast("date"), "date_of_birth") / 12)
    return customers.select(
        F.xxhash64("customer_id").alias("customer_sk"),
        "customer_id",
        F.sha2("email", 256).alias("email_sha256"),
        F.year("date_of_birth").alias("birth_year"),
        F.when(age < 30, "18-29").when(age < 45, "30-44").when(age < 60, "45-59")
        .otherwise("60+").alias("age_band"),
        "state", "segment", F.col("updated_at").alias("source_updated_at"),
    )


def build_dim_date(claims: DataFrame, policies: DataFrame) -> DataFrame:
    bounds = claims.select(F.col("loss_date").alias("d")) \
        .unionByName(policies.select(F.col("policy_start").alias("d"))) \
        .unionByName(policies.select(F.col("policy_end").alias("d"))) \
        .agg(F.min("d").alias("lo"), F.max("d").alias("hi"))
    days = bounds.select(F.explode(F.sequence("lo", "hi")).alias("calendar_date"))
    return days.select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
        "calendar_date",
        F.year("calendar_date").alias("year"),
        F.quarter("calendar_date").alias("quarter"),
        F.month("calendar_date").alias("month"),
        F.date_format("calendar_date", "MMMM").alias("month_name"),
        F.trunc("calendar_date", "month").alias("month_start"),
        F.dayofweek("calendar_date").alias("day_of_week"),
        F.dayofweek("calendar_date").isin(1, 7).alias("is_weekend"),
    )


def build_fact_claims(claims: DataFrame, dim_policy: DataFrame) -> DataFrame:
    """Point-in-time join: each claim binds to the policy version in force at loss."""
    loss_ts = F.col("c.loss_date").cast("timestamp")
    joined = claims.alias("c").join(
        dim_policy.alias("p"),
        (F.col("c.policy_id") == F.col("p.policy_id"))
        & (loss_ts >= F.col("p.effective_from")) & (loss_ts < F.col("p.effective_to")),
        "left",
    )
    return joined.select(
        F.col("c.claim_id"),
        F.col("p.policy_sk"),
        F.xxhash64(F.col("p.customer_id")).alias("customer_sk"),
        F.col("c.policy_id"),
        F.col("p.product_line"),
        F.date_format("c.loss_date", "yyyyMMdd").cast("int").alias("loss_date_key"),
        F.date_format("c.reported_date", "yyyyMMdd").cast("int").alias("reported_date_key"),
        F.col("c.claim_amount"),
        F.col("c.paid_amount"),
        (F.col("c.claim_amount") - F.col("c.paid_amount")).alias("outstanding_amount"),
        F.col("c.claim_status"),
        F.col("c.cause_of_loss"),
        F.col("c.last_modified"),
        F.col("c.batch_id"),
    )


def build_kpis(fact: DataFrame, dim_policy: DataFrame, dim_date: DataFrame) -> DataFrame:
    """Loss ratio, claim frequency and average severity by product line & month.

    * earned premium  = annual premium / 12 for each ACTIVE version in force at
      month end whose term overlaps the month
    * incurred losses = claim_amount of non-DENIED claims by loss month
    """
    months = fact.join(dim_date, fact.loss_date_key == dim_date.date_key) \
        .select("month_start").distinct() \
        .withColumn("month_end", F.last_day("month_start")) \
        .withColumn("as_of", F.date_add("month_end", 1).cast("timestamp"))
    in_force = months.join(
        dim_policy,
        (F.col("effective_from") < F.col("as_of")) & (F.col("effective_to") >= F.col("as_of"))
        & (F.col("status") == "ACTIVE")
        & (F.col("policy_start") <= F.col("month_end"))
        & (F.col("policy_end") >= F.col("month_start")),
    ).groupBy("month_start", "product_line").agg(
        F.count("*").alias("policies_in_force"),
        (F.sum("premium_annual") / 12).alias("earned_premium"),
    )
    losses = fact.filter(F.col("claim_status") != "DENIED") \
        .join(dim_date, fact.loss_date_key == dim_date.date_key) \
        .groupBy("month_start", "product_line").agg(
            F.count("*").alias("claim_count"),
            F.sum("claim_amount").alias("incurred_losses"),
            F.sum("paid_amount").alias("paid_losses"),
        )
    kpi = in_force.join(losses, ["month_start", "product_line"], "full_outer").fillna(
        0, subset=["policies_in_force", "claim_count"])
    return kpi.select(
        "month_start", "product_line", "policies_in_force", "claim_count",
        F.round("earned_premium", 2).cast("decimal(18,2)").alias("earned_premium"),
        F.coalesce("incurred_losses", F.lit(0)).cast("decimal(18,2)").alias("incurred_losses"),
        F.coalesce("paid_losses", F.lit(0)).cast("decimal(18,2)").alias("paid_losses"),
        # try_divide: ANSI mode raises on /0, a month/line may have no policies or claims
        F.round(F.try_divide("incurred_losses", "earned_premium"), 4).cast("double")
        .alias("loss_ratio"),
        F.round(F.try_divide("claim_count", "policies_in_force"), 4).cast("double")
        .alias("claim_frequency"),
        F.round(F.try_divide("incurred_losses", "claim_count"), 2).cast("decimal(18,2)")
        .alias("avg_severity"),
    ).orderBy("month_start", "product_line")


def stage_gold(spark: SparkSession, settings: Settings) -> dict[str, Path]:
    """Build every gold table from silver into staging; return staging paths."""
    customers = read_table(spark, settings.silver / "customers")
    dim_policy = read_table(spark, settings.silver / "dim_policy")
    claims = read_table(spark, settings.silver / "claims")
    staged: dict[str, Path] = {}
    gold = settings.gold
    staged["dim_date"] = stage(build_dim_date(claims, dim_policy), gold / "dim_date")
    staged["dim_customer"] = stage(build_dim_customer(customers), gold / "dim_customer")
    staged["dim_policy"] = stage(dim_policy, gold / "dim_policy")
    fact = build_fact_claims(claims, dim_policy)
    staged["fact_claims"] = stage(fact, gold / "fact_claims")
    fact_s = read_table(spark, staged["fact_claims"])
    date_s = read_table(spark, staged["dim_date"])
    staged["kpi_loss_ratio_monthly"] = stage(build_kpis(fact_s, dim_policy, date_s),
                                             gold / "kpi_loss_ratio_monthly")
    return staged


def promote_gold(settings: Settings, staged: dict[str, Path]) -> None:
    for name, path in staged.items():
        promote(path, settings.gold / name)
