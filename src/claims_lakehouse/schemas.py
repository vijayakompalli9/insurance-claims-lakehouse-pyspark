"""Explicit landing schemas. Bronze keeps every business field as STRING so
that type problems surface as reject reasons in silver instead of silent nulls.
"""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

CORRUPT_COL = "_corrupt_record"


def _strings(*names: str) -> StructType:
    fields = [StructField(n, StringType(), True) for n in names]
    return StructType(fields + [StructField(CORRUPT_COL, StringType(), True)])


CUSTOMERS = _strings("customer_id", "first_name", "last_name", "date_of_birth", "email",
                     "state", "segment", "updated_at")
POLICIES = _strings("policy_id", "customer_id", "product_line", "premium_annual",
                    "coverage_limit", "deductible", "status", "policy_start", "policy_end",
                    "region", "updated_at")
CLAIMS = _strings("claim_id", "policy_id", "loss_date", "reported_date", "claim_amount",
                  "paid_amount", "claim_status", "cause_of_loss", "last_modified")

# entity -> (file name, reader format, schema, primary key)
FEEDS: dict[str, tuple[str, str, StructType, str]] = {
    "customers": ("customers.csv", "csv", CUSTOMERS, "customer_id"),
    "policies": ("policies.json", "json", POLICIES, "policy_id"),
    "claims": ("claims.csv", "csv", CLAIMS, "claim_id"),
}

REJECT_REASONS = {
    "MALFORMED_RECORD": "row could not be parsed against the landing schema",
    "NULL_KEY": "primary key is null or blank",
    "INVALID_DATE": "date/timestamp not parseable or chronologically impossible",
    "INVALID_NUMBER": "numeric field not parseable",
    "NEGATIVE_AMOUNT": "monetary amount below zero",
    "DUPLICATE_KEY": "superseded duplicate of the same business key in this batch",
    "UNKNOWN_POLICY": "claim references a policy not present in dim_policy",
    "UNKNOWN_CUSTOMER": "policy references a customer not present in silver customers",
}
