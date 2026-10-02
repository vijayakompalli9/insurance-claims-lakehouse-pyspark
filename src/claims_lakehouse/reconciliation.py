"""Post-load reconciliation: counts, control totals and reject accountability.

Source-side figures are computed with the Python stdlib directly from the raw
files, independently of Spark, so the check is not "Spark agreeing with itself".
"""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .bronze import BronzeResult
from .config import Settings
from .schemas import CLAIMS, CORRUPT_COL, FEEDS
from .silver import SilverResult
from .storage import read_table

CLAIM_FIELDS = [f.name for f in CLAIMS.fields if f.name != CORRUPT_COL]


@dataclass
class Check:
    name: str
    entity: str
    expected: str
    actual: str
    passed: bool


def _dec(value: str | None) -> Decimal | None:
    try:
        d = Decimal((value or "").strip())
    except InvalidOperation:
        return None
    return d if d.is_finite() else None


def source_profile(raw_dir: Path) -> dict[str, Any]:
    """Row counts per feed plus the claim_amount control total of well-formed rows."""
    profile: dict[str, Any] = {}
    for entity, (file_name, fmt, _, _) in FEEDS.items():
        path = raw_dir / file_name
        if fmt == "json":
            profile[entity] = sum(1 for line in path.read_text().splitlines() if line.strip())
            continue
        with path.open(newline="") as fh:
            rows = list(csv.reader(fh))[1:]
        profile[entity] = len(rows)
        if entity == "claims":
            idx = CLAIM_FIELDS.index("claim_amount")
            total = sum((d for r in rows if len(r) == len(CLAIM_FIELDS)
                         if (d := _dec(r[idx])) is not None), Decimal("0"))
            profile["claims_amount_total"] = total
    return profile


def _check(name: str, entity: str, expected: Any, actual: Any) -> Check:
    return Check(name, entity, str(expected), str(actual), expected == actual)


def reconcile(spark: SparkSession, settings: Settings, batch_id: int,
              source: dict[str, Any], bronze: dict[str, BronzeResult],
              silver: dict[str, SilverResult]) -> dict[str, Any]:
    """Run all reconciliation checks for one batch and return the report."""
    checks: list[Check] = []
    for entity in FEEDS:
        b, s = bronze[entity], silver[entity]
        checks.append(_check("source_eq_bronze_plus_rejects", entity, source[entity],
                             b.accepted_count + b.rejected_count))
        checks.append(_check("bronze_eq_silver_plus_rejects_plus_stale", entity,
                             b.accepted_count, s.accepted + s.rejected + s.skipped_stale))
        rejects = read_table(spark, settings.rejects / entity) \
            .filter(F.col("batch_id") == batch_id).count()
        checks.append(_check("rejects_area_accounts_for_all_rejects", entity,
                             b.rejected_count + s.rejected, rejects))

    claims_silver = read_table(spark, settings.silver / "claims") \
        .filter(F.col("batch_id") == batch_id)
    fact = read_table(spark, settings.gold / "fact_claims").filter(F.col("batch_id") == batch_id)
    silver_total = claims_silver.agg(F.sum("claim_amount")).first()[0] or Decimal("0")
    fact_total = fact.agg(F.sum("claim_amount")).first()[0] or Decimal("0")
    checks.append(_check("silver_batch_rows_landed_in_fact", "claims",
                         silver["claims"].accepted, fact.count()))
    checks.append(_check("control_total_silver_eq_fact", "claims", silver_total, fact_total))

    # well-formed source amount = accepted amount + parseable amount on rejected rows
    rej = read_table(spark, settings.rejects / "claims").filter(
        (F.col("batch_id") == batch_id) & (F.col("reason_code") != "MALFORMED_RECORD"))
    rej_total = rej.select(F.get_json_object("raw_payload", "$.claim_amount")
                           .try_cast("decimal(14,2)").alias("a")) \
        .agg(F.sum("a")).first()[0] or Decimal("0")
    checks.append(_check("control_total_source_eq_accepted_plus_rejected", "claims",
                         source["claims_amount_total"], silver_total + rej_total))

    reasons = {e: dict(silver[e].rejects_by_reason) for e in FEEDS}
    for e in FEEDS:
        malformed_or_null = read_table(spark, settings.rejects / e).filter(
            (F.col("batch_id") == batch_id) & (F.col("layer") == "bronze")) \
            .groupBy("reason_code").count().collect()
        for row in malformed_or_null:
            reasons[e][row["reason_code"]] = row["count"]
    return {
        "batch_id": batch_id,
        "status": "PASSED" if all(c.passed for c in checks) else "FAILED",
        "checks": [asdict(c) for c in checks],
        "rejects_by_entity_and_reason": {e: dict(sorted(r.items())) for e, r in reasons.items()},
        "claims_amount": {"source_well_formed": str(source["claims_amount_total"]),
                          "accepted": str(silver_total), "rejected_parseable": str(rej_total)},
    }
