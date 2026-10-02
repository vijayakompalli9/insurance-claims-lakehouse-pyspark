"""SCD Type 2 correctness across two batches (hand-built inputs)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession

from claims_lakehouse.scd2 import OPEN_END, apply_scd2

COLS = ["policy_id", "customer_id", "product_line", "premium_annual", "coverage_limit",
        "deductible", "status", "policy_start", "policy_end", "region", "updated_at"]


def _rows(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    schema = ("policy_id string, customer_id string, product_line string, "
              "premium_annual decimal(12,2), coverage_limit decimal(14,2), "
              "deductible decimal(10,2), status string, policy_start date, policy_end date, "
              "region string, updated_at timestamp")
    return spark.createDataFrame(rows, schema)


def _pol(pid: str, premium: str, updated: datetime, status: str = "ACTIVE") -> tuple:
    return (pid, "C1", "AUTO", Decimal(premium), Decimal("100000"), Decimal("500"), status,
            date(2025, 6, 1), date(2026, 6, 1), "WEST", updated)


@pytest.fixture()
def two_batches(spark: SparkSession):
    b1 = _rows(spark, [_pol("P1", "1000.00", datetime(2026, 1, 10)),
                       _pol("P2", "2000.00", datetime(2026, 1, 11)),
                       _pol("P3", "3000.00", datetime(2026, 1, 12))])
    dim1 = apply_scd2(None, b1, batch_id=1).cache()
    b2 = _rows(spark, [
        _pol("P1", "1100.00", datetime(2026, 2, 5)),                       # change
        _pol("P2", "2000.00", datetime(2026, 2, 6)),                       # touched, no change
        _pol("P3", "3300.00", datetime(2026, 2, 7)),                       # change ...
        _pol("P3", "3300.00", datetime(2026, 2, 9), status="CANCELLED"),   # ... and again
        _pol("P4", "4000.00", datetime(2026, 2, 8)),                       # new policy
    ])
    dim2 = apply_scd2(dim1, b2, batch_id=2).cache()
    return dim1, dim2, b2


def _versions(dim: DataFrame, pid: str) -> list:
    return dim.filter(f"policy_id = '{pid}'").orderBy("effective_from").collect()


def test_initial_load_opens_one_current_version_from_policy_start(two_batches):
    dim1, _, _ = two_batches
    assert dim1.count() == 3
    for row in dim1.collect():
        assert row.is_current
        assert row.effective_from == datetime(2025, 6, 1)
        assert str(row.effective_to) == OPEN_END


def test_change_closes_old_version_and_opens_new(two_batches):
    _, dim2, _ = two_batches
    v = _versions(dim2, "P1")
    assert [r.premium_annual for r in v] == [Decimal("1000.00"), Decimal("1100.00")]
    assert v[0].effective_to == v[1].effective_from == datetime(2026, 2, 5)
    assert [r.is_current for r in v] == [False, True]
    assert v[0].hash_diff != v[1].hash_diff
    assert v[1].batch_id == 2


def test_hash_diff_suppresses_touch_without_attribute_change(two_batches):
    _, dim2, _ = two_batches
    v = _versions(dim2, "P2")
    assert len(v) == 1 and v[0].is_current and v[0].batch_id == 1


def test_multiple_changes_in_one_batch_are_chained_in_order(two_batches):
    _, dim2, _ = two_batches
    v = _versions(dim2, "P3")
    assert [r.status for r in v] == ["ACTIVE", "ACTIVE", "CANCELLED"]
    assert v[0].effective_to == v[1].effective_from
    assert v[1].effective_to == v[2].effective_from
    assert [r.is_current for r in v] == [False, False, True]


def test_new_policy_in_batch_two_and_dimension_invariants(two_batches):
    _, dim2, _ = two_batches
    v = _versions(dim2, "P4")
    assert len(v) == 1 and v[0].effective_from == datetime(2025, 6, 1)
    assert dim2.count() == 7  # 3 initial + P1 + P3 x2 + P4
    assert dim2.filter("is_current").groupBy("policy_id").count().filter("count > 1").count() == 0
    assert dim2.select("policy_sk").distinct().count() == dim2.count()


def test_replaying_batch_two_is_idempotent(two_batches):
    _, dim2, b2 = two_batches
    dim3 = apply_scd2(dim2, b2, batch_id=2)
    key = ["policy_sk", "effective_from", "effective_to", "is_current", "hash_diff"]
    assert sorted(dim3.select(key).collect()) == sorted(dim2.select(key).collect())
