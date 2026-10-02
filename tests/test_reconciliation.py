"""Source profiling math used by post-load reconciliation."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from claims_lakehouse.reconciliation import source_profile

HEADER = ("claim_id,policy_id,loss_date,reported_date,claim_amount,paid_amount,claim_status,"
          "cause_of_loss,last_modified\n")


def test_source_profile_counts_and_control_total(tmp_path: Path):
    (tmp_path / "customers.csv").write_text("customer_id,first_name\nC1,A\nC2,B\n")
    (tmp_path / "policies.json").write_text(
        json.dumps({"policy_id": "P1"}) + "\n" + '{"broken": \n' + "\n")
    (tmp_path / "claims.csv").write_text(
        HEADER
        + "K1,P1,2026-01-01,2026-01-02,100.10,0,OPEN,FIRE,2026-01-02 00:00:00\n"
        + "K2,P1,2026-01-01,2026-01-02,-0.10,0,OPEN,FIRE,2026-01-02 00:00:00\n"
        + "K3,P1,2026-01-01,2026-01-02,12;5,0,OPEN,FIRE,2026-01-02 00:00:00\n"
        + "K4,P1,2026-01-01,2026-01-02,999.99,0,OPEN,FIRE,2026-01-02 00:00:00,EXTRA\n"
        + ",P1,2026-01-01,2026-01-02,0.20,0,OPEN,FIRE,2026-01-02 00:00:00\n")
    prof = source_profile(tmp_path)
    assert prof["customers"] == 2
    assert prof["policies"] == 2            # blank lines ignored, broken line counted
    assert prof["claims"] == 5              # every data row, malformed included
    # malformed (K4) and unparseable (K3) amounts excluded; exact decimal arithmetic
    assert prof["claims_amount_total"] == Decimal("100.20")
