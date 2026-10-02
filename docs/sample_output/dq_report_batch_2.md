# DQ report - batch 2

Gate: **PASSED** - 15/15 rules passed, 0 critical failure(s), 0 warning(s)

| rule | table | type | severity | failed / total | passed |
|---|---|---|---|---|---|
| claims_claim_id_not_null | silver.claims | not_null | critical | 0 / 160 | yes |
| claims_claim_id_unique | silver.claims | unique | critical | 0 / 160 | yes |
| claims_amount_range | silver.claims | range | critical | 0 / 160 | yes |
| claims_status_accepted | silver.claims | accepted_values | critical | 0 / 160 | yes |
| claims_policy_referential | silver.claims | referential | critical | 0 / 160 | yes |
| policy_one_current_version | silver.dim_policy | unique | critical | 0 / 1351 | yes |
| policy_version_key_unique | silver.dim_policy | unique | critical | 0 / 1392 | yes |
| policy_product_line_accepted | silver.dim_policy | accepted_values | critical | 0 / 1392 | yes |
| policy_premium_range | silver.dim_policy | range | warning | 0 / 1392 | yes |
| customer_segment_accepted | silver.customers | accepted_values | warning | 0 / 1015 | yes |
| fact_policy_sk_resolves | gold.fact_claims | referential | critical | 0 / 160 | yes |
| fact_customer_sk_resolves | gold.fact_claims | referential | critical | 0 / 160 | yes |
| fact_loss_date_resolves | gold.fact_claims | referential | critical | 0 / 160 | yes |
| fact_paid_not_above_incurred | gold.fact_claims | range | warning | 0 / 160 | yes |
| kpi_loss_ratio_sane | gold.kpi_loss_ratio_monthly | range | warning | 0 / 8 | yes |
