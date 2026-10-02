"""Bad rows are quarantined with the right reason codes and never reach silver."""

from __future__ import annotations

from pyspark.sql import functions as F

from claims_lakehouse.bronze import ingest
from claims_lakehouse.silver import process_claims, process_customers, process_policies
from claims_lakehouse.storage import read_table


def test_batch_one_reject_reasons(spark, settings):
    b = {e: ingest(spark, settings, e, 1) for e in ("customers", "policies", "claims")}
    process_customers(spark, settings, b["customers"].accepted)
    process_policies(spark, settings, b["policies"].accepted, None, 1)
    result = process_claims(spark, settings, b["claims"].accepted)

    rejects = read_table(spark, settings.rejects / "claims")
    counts = rejects.groupBy("reason_code").count().collect()
    by_reason = {r.reason_code: r["count"] for r in counts}
    assert by_reason == {"MALFORMED_RECORD": 1, "NULL_KEY": 3, "NEGATIVE_AMOUNT": 3,
                         "UNKNOWN_POLICY": 3, "INVALID_DATE": 3, "INVALID_NUMBER": 1,
                         "DUPLICATE_KEY": 2}
    layer = {r.reason_code: r.layer for r in rejects.collect()}
    assert layer["MALFORMED_RECORD"] == layer["NULL_KEY"] == "bronze"
    assert layer["UNKNOWN_POLICY"] == "silver"
    for key, reason in [("CLMBDT100", "INVALID_DATE"), ("CLMBDT101", "INVALID_DATE"),
                        ("CLMBDT102", "INVALID_DATE"), ("CLMNEG100", "NEGATIVE_AMOUNT"),
                        ("CLMUNK100", "UNKNOWN_POLICY"), ("CLMNAN100", "INVALID_NUMBER")]:
        assert rejects.filter(F.col("business_key") == key).first().reason_code == reason
    assert rejects.filter("raw_payload is null or source_file is null").count() == 0

    silver = read_table(spark, settings.silver / "claims")
    assert silver.count() == result.accepted == 80
    assert silver.filter("claim_amount < 0 or claim_id is null").count() == 0
    assert silver.filter(F.col("claim_id").rlike("^CLM(NEG|UNK|BDT|NAN)")).count() == 0

    pol_rejects = read_table(spark, settings.rejects / "policies")
    assert sorted(r.reason_code for r in pol_rejects.collect()) == ["MALFORMED_RECORD", "NULL_KEY"]
