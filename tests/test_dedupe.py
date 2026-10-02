"""Window-function dedupe keeps the newest record and reports the rest."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import functions as F

from claims_lakehouse.silver import dedupe_latest, first_failure


def test_dedupe_keeps_latest_and_flags_superseded(spark):
    df = spark.createDataFrame([
        ("A", "OPEN", datetime(2026, 1, 1, 8)),
        ("A", "CLOSED", datetime(2026, 1, 2, 8)),
        ("B", "OPEN", datetime(2026, 1, 1, 9)),
        ("B", "OPEN", datetime(2026, 1, 1, 9)),   # exact resend
        ("C", "DENIED", datetime(2026, 1, 3, 9)),
    ], "claim_id string, status string, modified timestamp")
    kept, dupes = dedupe_latest(df, "claim_id", "modified")
    got = {r.claim_id: r.status for r in kept.collect()}
    assert got == {"A": "CLOSED", "B": "OPEN", "C": "DENIED"}
    assert dupes.count() == 2
    assert {r.reason_code for r in dupes.collect()} == {"DUPLICATE_KEY"}
    assert dupes.filter("claim_id = 'A'").first().status == "OPEN"


def test_dedupe_is_deterministic_on_ties(spark):
    df = spark.createDataFrame([("A", "x", 1), ("A", "y", 1), ("A", "z", 1)],
                               "k string, v string, ts int")
    picks = {dedupe_latest(df.repartition(3), "k", "ts")[0].first().v for _ in range(3)}
    assert len(picks) == 1


def test_first_failure_returns_first_matching_reason(spark):
    df = spark.createDataFrame([(-1, None), (5, None), (-1, "bad")], "amt int, d string")
    reason = first_failure([(F.col("d") == "bad", "INVALID_DATE"),
                            (F.col("amt") < 0, "NEGATIVE_AMOUNT")])
    assert [r.r for r in df.select(reason.alias("r")).collect()] == \
        ["NEGATIVE_AMOUNT", None, "INVALID_DATE"]
