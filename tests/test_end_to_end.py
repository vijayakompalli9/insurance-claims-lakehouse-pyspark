"""Full CLI run over both batches plus replay / ordering guarantees."""

from __future__ import annotations

import json
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from claims_lakehouse.config import Settings
from claims_lakehouse.run import main
from claims_lakehouse.state import PipelineState
from claims_lakehouse.storage import read_table

from .conftest import RULES


@pytest.fixture()
def cli_env(monkeypatch, settings: Settings):
    monkeypatch.setenv("CLAIMS_RAW_ROOT", str(settings.raw_root))
    monkeypatch.setenv("CLAIMS_LAKEHOUSE_ROOT", str(settings.lakehouse_root))
    return settings


def test_full_run_both_batches(spark, cli_env, capsys):
    s = cli_env
    assert main(["--batch", "all", "--rules", str(RULES)], spark=spark) == 0
    out = capsys.readouterr().out
    assert "batch 2 | DQ gate PASSED" in out

    state = PipelineState.load(s.state_file)
    assert state.published_batches == [1, 2]
    assert state.policy_watermark.startswith("2026-02")
    for b in (1, 2):
        recon = json.loads((s.reports / f"reconciliation_batch_{b}.json").read_text())
        assert recon["status"] == "PASSED", recon
        dq_report = json.loads((s.reports / f"dq_report_batch_{b}.json").read_text())
        assert dq_report["gate"] == "PASSED"

    fact = read_table(spark, s.gold / "fact_claims")
    dim = read_table(spark, s.gold / "dim_policy")
    assert fact.count() == 160                 # 80 + 90 - 10 lifecycle updates
    assert fact.select("claim_id").distinct().count() == fact.count()
    assert fact.filter("policy_sk is null or customer_sk is null").count() == 0
    assert dim.count() == 1336 + 15 + 41       # initial + new + changed versions
    assert dim.filter("is_current").count() == 1351

    # point-in-time binding: claims bind to the version in force on the loss date
    bound = fact.join(dim, "policy_sk").filter(
        (F.col("loss_date_key") < F.date_format("effective_from", "yyyyMMdd").cast("int"))
        | (F.col("loss_date_key") >= F.date_format("effective_to", "yyyyMMdd").cast("int")))
    assert bound.count() == 0
    updated = fact.filter("batch_id = 2 and claim_id < 'CLM0000081'")
    assert updated.count() == 10 and updated.filter("claim_status = 'CLOSED'").count() == 10

    kpi = read_table(spark, s.gold / "kpi_loss_ratio_monthly")
    assert kpi.count() == 8
    assert kpi.filter("loss_ratio is null or earned_premium <= 0").count() == 0
    assert not (s.gold / "dim_customer").joinpath("email").exists()
    assert "email" not in read_table(spark, s.gold / "dim_customer").columns


def test_replay_latest_batch_is_idempotent_and_order_enforced(spark, cli_env):
    s = cli_env
    assert main(["--batch", "all", "--rules", str(RULES)], spark=spark) == 0
    before = {t: read_table(spark, s.gold / t).count() for t in ("fact_claims", "dim_policy")}
    assert main(["--batch", "2", "--rules", str(RULES)], spark=spark) == 0
    after = {t: read_table(spark, s.gold / t).count() for t in ("fact_claims", "dim_policy")}
    assert before == after
    assert main(["--batch", "1", "--rules", str(RULES)], spark=spark) == 1  # BatchOrderError
    assert datetime.fromisoformat(PipelineState.load(s.state_file).policy_watermark)
