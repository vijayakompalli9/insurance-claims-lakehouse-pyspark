"""Config-driven data-quality rules engine with a circuit breaker.

Rules live in YAML (see ``config/dq_rules.yaml``). Supported types:
``not_null``, ``unique``, ``range``, ``accepted_values``, ``referential``.
Every rule may add ``where`` (SQL filter), ``severity`` (critical|warning) and
``max_failure_pct`` (tolerated share of failing rows, default 0).
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .exceptions import ConfigError
from .logging_utils import log_kv

log = logging.getLogger(__name__)
RULE_TYPES = {"not_null", "unique", "range", "accepted_values", "referential"}
SEVERITIES = {"critical", "warning"}


@dataclass
class RuleResult:
    name: str
    table: str
    rule_type: str
    severity: str
    total_rows: int
    failed_rows: int
    failure_pct: float
    passed: bool
    detail: str


def load_rules(path: Path) -> list[dict[str, Any]]:
    """Load and validate the rules file."""
    try:
        rules = (yaml.safe_load(path.read_text()) or {}).get("rules", [])
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"cannot read DQ rules {path}: {exc}") from exc
    for rule in rules:
        missing = {"name", "table", "type"} - rule.keys()
        if missing:
            raise ConfigError(f"rule {rule} missing {sorted(missing)}")
        if rule["type"] not in RULE_TYPES:
            raise ConfigError(f"rule {rule['name']}: unknown type {rule['type']}")
        if rule.setdefault("severity", "critical") not in SEVERITIES:
            raise ConfigError(f"rule {rule['name']}: bad severity {rule['severity']}")
    return rules


def _failures(rule: dict[str, Any], df: DataFrame, tables: dict[str, DataFrame]) -> int:
    kind = rule["type"]
    if kind == "not_null":
        cond = None
        for c in rule.get("columns", [rule.get("column")]):
            cond = F.col(c).isNull() if cond is None else cond | F.col(c).isNull()
        return df.filter(cond).count()
    if kind == "unique":
        cols = rule.get("columns", [rule.get("column")])
        dup_groups = df.groupBy(*cols).count().filter("count > 1")
        return int(dup_groups.agg(F.sum("count")).first()[0] or 0)
    if kind == "range":
        col = F.col(rule["column"])
        cond = F.lit(False)
        if "min" in rule:
            cond = cond | (col < F.lit(rule["min"]))
        if "max" in rule:
            cond = cond | (col > F.lit(rule["max"]))
        return df.filter(col.isNotNull() & cond).count()
    if kind == "accepted_values":
        col = F.col(rule["column"])
        return df.filter(col.isNotNull() & ~col.isin(rule["values"])).count()
    # referential
    ref = tables.get(rule["ref_table"])
    if ref is None:
        raise ConfigError(f"rule {rule['name']}: unknown ref_table {rule['ref_table']}")
    keys = ref.select(F.col(rule["ref_column"]).alias("_ref")).distinct()
    left = df.select(F.col(rule["column"]).alias("_key"))
    orphans = left.join(keys, left._key == keys._ref, "left_anti")
    return orphans.count()  # null keys never match, so they count as orphans


def evaluate(rules: list[dict[str, Any]], tables: dict[str, DataFrame]) -> list[RuleResult]:
    """Run every rule against the named tables and return results."""
    results: list[RuleResult] = []
    for rule in rules:
        if rule["table"] not in tables:
            raise ConfigError(f"rule {rule['name']}: unknown table {rule['table']}")
        df = tables[rule["table"]]
        if rule.get("where"):
            df = df.filter(rule["where"])
        total = df.count()
        failed = _failures(rule, df, tables)
        pct = round(100.0 * failed / total, 3) if total else 0.0
        passed = pct <= float(rule.get("max_failure_pct", 0))
        detail = {k: v for k, v in rule.items()
                  if k not in {"name", "table", "type", "severity", "description"}}
        result = RuleResult(rule["name"], rule["table"], rule["type"], rule["severity"],
                            total, failed, pct, passed, str(detail))
        results.append(result)
        log_kv(log, "dq rule evaluated", level=logging.INFO if passed else logging.WARNING,
               rule=result.name, severity=result.severity, failed=failed, total=total,
               passed=passed)
    return results


def critical_failures(results: list[RuleResult]) -> list[RuleResult]:
    return [r for r in results if r.severity == "critical" and not r.passed]


def build_report(batch_id: int, results: list[RuleResult]) -> dict[str, Any]:
    crit = critical_failures(results)
    return {
        "batch_id": batch_id,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "rules_evaluated": len(results),
        "rules_passed": sum(r.passed for r in results),
        "critical_failures": len(crit),
        "warnings": sum(1 for r in results if r.severity == "warning" and not r.passed),
        "gate": "BLOCKED" if crit else "PASSED",
        "results": [asdict(r) for r in results],
    }


def build_alert(batch_id: int, results: list[RuleResult]) -> dict[str, Any]:
    """Alert payload suitable for a webhook / SNS / Teams adapter (not sent here)."""
    return {
        "alert_type": "DQ_CIRCUIT_BREAKER_OPEN",
        "severity": "critical",
        "pipeline": "insurance-claims-lakehouse",
        "batch_id": batch_id,
        "action_taken": "gold publish blocked; staged gold discarded; watermark not advanced",
        "failed_rules": [
            {"rule": r.name, "table": r.table, "type": r.rule_type,
             "failed_rows": r.failed_rows, "failure_pct": r.failure_pct}
            for r in critical_failures(results)
        ],
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "runbook": "inspect rejects + DQ report, fix source or rule, re-run the same batch",
    }


def report_markdown(report: dict[str, Any]) -> str:
    lines = [f"# DQ report - batch {report['batch_id']}", "",
             f"Gate: **{report['gate']}** - {report['rules_passed']}/{report['rules_evaluated']}"
             f" rules passed, {report['critical_failures']} critical failure(s), "
             f"{report['warnings']} warning(s)", "",
             "| rule | table | type | severity | failed / total | passed |",
             "|---|---|---|---|---|---|"]
    for r in report["results"]:
        lines.append(f"| {r['name']} | {r['table']} | {r['rule_type']} | {r['severity']} | "
                     f"{r['failed_rows']} / {r['total_rows']} | {'yes' if r['passed'] else 'NO'} |")
    return "\n".join(lines) + "\n"
