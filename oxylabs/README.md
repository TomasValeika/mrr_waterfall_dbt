# oxylabs dbt Project

This dbt project models the raw SaaS task datasets into a monthly MRR waterfall.

## Models

- `models/source/sources.yml`: raw source declarations in schema `raw`
- `models/bronze/`: source-aligned staging views
- `models/silver/`: typed business entities and account-month MRR spine
- `models/gold/gold_mrr_waterfall.sql`: monthly revenue movement mart

## Business Rules

- Trial subscriptions do not contribute to MRR.
- Subscription MRR is recognized from the subscription start month through the end month, inclusive.
- The account-month spine is bounded by the dataset snapshot, not the machine clock, so results stay stable across reruns.
- Waterfall categories are `new`, `reactivation`, `expansion`, `contraction`, and `churn`.

## Validation

- Generic not-null and uniqueness tests on stable identifiers.
- Custom tests for account-month uniqueness, non-negative MRR, and waterfall reconciliation.
- `feature_usage.usage_id` is only tested for `not_null`, because the provided raw file contains duplicate ids.

## Run

```bash
uv run dbt run --project-dir oxylabs --profiles-dir oxylabs
uv run dbt test --project-dir oxylabs --profiles-dir oxylabs
```
