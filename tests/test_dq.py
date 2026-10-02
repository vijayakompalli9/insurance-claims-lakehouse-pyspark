"""Rules engine semantics and the gold-publishing circuit breaker."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from claims_lakehouse import dq
from claims_lakehouse.exceptions import ConfigError, DataQualityCircuitBreakerError
from claims_lakehouse.run import run_batch
from claims_lakehouse.state import PipelineState
from claims_lakehouse.storage import table_exists


@pytest.fixture()
def tables(spark):
    claims = spark.createDataFrame(
        [("c1", "p1", 10.0, "OPEN"), ("c2", "p9", -5.0, "LOST"), ("c2", None, 3.0, "OPEN"),
         (None, "p1", 7.0, "CLOSED")], "claim_id string, policy_id string, amt double, s string")
    policies = spark.createDataFrame([("p1",), ("p2",)], "policy_id string")
    return {"claims": claims, "policies": policies}


@pytest.mark.parametrize(("rule", "expected_failed"), [
    ({"type": "not_null", "column": "claim_id"}, 1),
    ({"type": "unique", "column": "claim_id"}, 2),
    ({"type": "range", "column": "amt", "min": 0, "max": 9}, 2),
    ({"type": "accepted_values", "column": "s", "values": ["OPEN", "CLOSED"]}, 1),
    ({"type": "referential", "column": "policy_id", "ref_table": "policies",
      "ref_column": "policy_id"}, 2),
    ({"type": "not_null", "column": "claim_id", "where": "s = 'OPEN'"}, 0),
])
def test_rule_types(tables, rule, expected_failed):
    [res] = dq.evaluate([{"name": "r", "table": "claims", "severity": "critical", **rule}], tables)
    assert res.failed_rows == expected_failed
    assert res.passed is (expected_failed == 0)


def test_tolerance_and_severity(tables):
    rules = [{"name": "tolerant", "table": "claims", "type": "not_null", "column": "claim_id",
              "severity": "critical", "max_failure_pct": 30},
             {"name": "warn", "table": "claims", "type": "unique", "column": "claim_id",
              "severity": "warning"}]
    results = dq.evaluate(rules, tables)
    assert [r.passed for r in results] == [True, False]
    assert dq.critical_failures(results) == []
    assert dq.build_report(1, results)["gate"] == "PASSED"


def test_invalid_rule_config_rejected(tmp_path: Path):
    path = tmp_path / "rules.yaml"
    path.write_text(yaml.safe_dump({"rules": [{"name": "x", "table": "t", "type": "magic"}]}))
    with pytest.raises(ConfigError):
        dq.load_rules(path)


def test_critical_failure_blocks_gold_and_emits_alert(spark, settings, tmp_path):
    strict = tmp_path / "strict_rules.yaml"
    strict.write_text(yaml.safe_dump({"rules": [{
        "name": "claims_amount_tiny", "table": "silver.claims", "type": "range",
        "column": "claim_amount", "max": 10, "severity": "critical"}]}))
    with pytest.raises(DataQualityCircuitBreakerError) as exc:
        run_batch(spark, settings, 1, rules_path=strict)

    alert = json.loads(Path(exc.value.alert_path).read_text())
    assert alert["alert_type"] == "DQ_CIRCUIT_BREAKER_OPEN"
    assert alert["failed_rules"][0]["rule"] == "claims_amount_tiny"
    assert alert["failed_rules"][0]["failed_rows"] > 0
    assert not table_exists(settings.gold / "fact_claims")          # nothing published
    assert not list(settings.gold.glob("*.__staging"))              # staging discarded
    assert PipelineState.load(settings.state_file).published_batches == []

    # fix the rule, re-run the same batch: publishes cleanly (idempotent recovery)
    summary = run_batch(spark, settings, 1)
    assert summary["dq_gate"] == "PASSED" and summary["reconciliation"] == "PASSED"
    assert table_exists(settings.gold / "fact_claims")
