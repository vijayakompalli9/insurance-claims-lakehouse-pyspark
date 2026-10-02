"""CLI entrypoint: ``python -m claims_lakehouse.run --batch 1|2|all``.

Per batch: bronze -> silver -> stage gold -> DQ gate -> publish gold ->
reconciliation -> advance watermark. Exit codes: 0 ok, 2 DQ circuit breaker,
3 reconciliation failure, 1 any other pipeline error.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from . import dq
from .bronze import ingest
from .config import Settings, load_settings
from .exceptions import (
    DataQualityCircuitBreakerError,
    LakehouseError,
    ReconciliationError,
)
from .generate_data import generate
from .gold import promote_gold, stage_gold
from .logging_utils import configure_logging, log_kv
from .reconciliation import reconcile, source_profile
from .silver import process_claims, process_customers, process_policies
from .spark import build_spark
from .state import PipelineState
from .storage import discard, read_table, write_json

log = logging.getLogger("claims_lakehouse.run")


def dq_tables(spark: SparkSession, settings: Settings,
              staged: dict[str, Path]) -> dict[str, Any]:
    tables = {f"silver.{t}": read_table(spark, settings.silver / t)
              for t in ("customers", "dim_policy", "claims")}
    tables.update({f"gold.{t}": read_table(spark, p) for t, p in staged.items()})
    return tables


def run_batch(spark: SparkSession, settings: Settings, batch_id: int,
              rules_path: Path | None = None) -> dict[str, Any]:
    """Process one batch end to end and return a run summary."""
    state = PipelineState.load(settings.state_file)
    state.check_can_run(batch_id)
    log_kv(log, "batch started", batch=batch_id, watermark=state.policy_watermark)
    source = source_profile(settings.raw_root / f"batch_{batch_id}")

    bronze = {e: ingest(spark, settings, e, batch_id)
              for e in ("customers", "policies", "claims")}
    silver = {"customers": process_customers(spark, settings, bronze["customers"].accepted)}
    silver["policies"], new_watermark = process_policies(
        spark, settings, bronze["policies"].accepted, state.policy_watermark, batch_id)
    silver["claims"] = process_claims(spark, settings, bronze["claims"].accepted)

    staged = stage_gold(spark, settings)
    results = dq.evaluate(dq.load_rules(rules_path or settings.rules_path),
                          dq_tables(spark, settings, staged))
    report = dq.build_report(batch_id, results)
    reports = settings.reports
    write_json(reports / f"dq_report_batch_{batch_id}.json", report)
    (reports / f"dq_report_batch_{batch_id}.md").write_text(dq.report_markdown(report))
    if dq.critical_failures(results):
        alert_path = reports / f"alert_batch_{batch_id}.json"
        write_json(alert_path, dq.build_alert(batch_id, results))
        for path in staged.values():
            discard(path)
        log_kv(log, "circuit breaker open - gold NOT published", level=logging.ERROR,
               batch=batch_id, alert=alert_path)
        raise DataQualityCircuitBreakerError(
            f"{len(dq.critical_failures(results))} critical DQ rule(s) failed", str(alert_path))
    promote_gold(settings, staged)
    log_kv(log, "gold published", batch=batch_id, tables=",".join(staged))

    recon = reconcile(spark, settings, batch_id, source, bronze, silver)
    write_json(reports / f"reconciliation_batch_{batch_id}.json", recon)
    if recon["status"] != "PASSED":
        raise ReconciliationError(f"batch {batch_id} reconciliation failed")

    if batch_id not in state.published_batches:
        state.published_batches.append(batch_id)
    state.policy_watermark = new_watermark
    state.save(settings.state_file)
    log_kv(log, "batch complete", batch=batch_id, watermark=new_watermark)
    return {"batch_id": batch_id, "dq_gate": report["gate"],
            "dq_passed": f"{report['rules_passed']}/{report['rules_evaluated']}",
            "reconciliation": recon["status"],
            "rows": {e: {"source": source[e], "bronze": bronze[e].accepted_count,
                         "silver": silver[e].accepted,
                         "rejected": bronze[e].rejected_count + silver[e].rejected,
                         "stale_skipped": silver[e].skipped_stale} for e in bronze}}


def print_summary(spark: SparkSession, settings: Settings, summaries: list[dict]) -> None:
    for s in summaries:
        print(f"\n== batch {s['batch_id']} | DQ gate {s['dq_gate']} ({s['dq_passed']}) "
              f"| reconciliation {s['reconciliation']}")
        print(f"{'entity':<10}{'source':>8}{'bronze':>8}{'silver':>8}{'rejected':>10}"
              f"{'stale':>7}")
        for e, r in s["rows"].items():
            print(f"{e:<10}{r['source']:>8}{r['bronze']:>8}{r['silver']:>8}{r['rejected']:>10}"
                  f"{r['stale_skipped']:>7}")
    print("\n== gold table row counts")
    for t in ("dim_date", "dim_customer", "dim_policy", "fact_claims", "kpi_loss_ratio_monthly"):
        print(f"{t:<24}{read_table(spark, settings.gold / t).count():>8}")
    print("\n== kpi_loss_ratio_monthly")
    read_table(spark, settings.gold / "kpi_loss_ratio_monthly") \
        .orderBy("month_start", "product_line").show(20, truncate=False)


def main(argv: list[str] | None = None, spark: SparkSession | None = None) -> int:
    """CLI main. ``spark`` lets tests inject a shared session (it is then not stopped)."""
    parser = argparse.ArgumentParser(description="Insurance claims lakehouse pipeline")
    parser.add_argument("--batch", required=True, choices=["1", "2", "all"])
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--rules", type=Path, default=None, help="override DQ rules file")
    parser.add_argument("--reset", action="store_true", help="wipe lakehouse before running")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    configure_logging(args.log_level)
    settings = load_settings(args.config)
    if args.reset and settings.lakehouse_root.exists():
        shutil.rmtree(settings.lakehouse_root)
    if not (settings.raw_root / "batch_1").exists():
        log_kv(log, "raw feeds missing - generating synthetic data", seed=settings.seed)
        generate(settings.raw_root, seed=settings.seed)

    owns_spark = spark is None
    spark = spark or build_spark(settings.spark_master, settings.shuffle_partitions)
    batches = [1, 2] if args.batch == "all" else [int(args.batch)]
    summaries = []
    try:
        for b in batches:
            summaries.append(run_batch(spark, settings, b, args.rules))
        print_summary(spark, settings, summaries)
        return 0
    except DataQualityCircuitBreakerError as exc:
        log_kv(log, str(exc), level=logging.ERROR, alert=exc.alert_path)
        return 2
    except ReconciliationError as exc:
        log_kv(log, str(exc), level=logging.ERROR)
        return 3
    except LakehouseError as exc:
        log_kv(log, str(exc), level=logging.ERROR, error=type(exc).__name__)
        return 1
    finally:
        if owns_spark:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
