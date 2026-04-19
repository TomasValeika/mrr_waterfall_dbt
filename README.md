# Technical Task

Build a dbt project that transforms raw SaaS data into a monthly MRR waterfall for finance and analytics use cases.

## Project Structure

- `data/`: source CSV files and the original task PDF
- `data_loader.py`: creates the `raw` schema, reloads CSV data, and clears dbt output schemas
- `docker-compose.yml`: local Postgres service
- `oxylabs/models/bronze`: source-aligned staging models
- `oxylabs/models/silver`: cleaned intermediate models and monthly account MRR spine
- `oxylabs/models/gold`: final monthly MRR waterfall model
- `oxylabs/tests`: custom business logic tests

## Deliverables

- Source declarations for all raw datasets
- Bronze models for each input table
- Silver models for typed business-ready entities
- Gold model `gold_mrr_waterfall` with monthly MRR movement categories
- Custom data tests for MRR sanity and waterfall reconciliation

## Modeling Decisions

- Bronze models select directly from dbt sources with minimal transformation.
- Silver models cast data types and prepare account-level monthly MRR.
- Trial subscriptions do not contribute to MRR.
- MRR is recognized for every month between subscription start and end month, inclusive.
- A full account-month spine is generated so zero-MRR months are preserved.
- Gold classifies MRR movement into `new`, `reactivation`, `expansion`, `contraction`, and `churn`.

## Assumptions

- `new_mrr` is the first month an account becomes paying.
- `reactivation_mrr` is a return to positive MRR after at least one zero-MRR month.
- `churn_mrr` is recorded when account MRR drops from positive to zero.
- Revenue is modeled at account-month granularity, not invoice or daily granularity.

## How To Run

Start Postgres:

```bash
docker compose up -d
```

Load the raw CSV files into the `raw` schema:

```bash
uv run python data_loader.py
```

Run dbt models:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run dbt run --project-dir oxylabs --profiles-dir oxylabs
```

Run dbt tests:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run dbt test --project-dir oxylabs --profiles-dir oxylabs
```

Optional validation:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run dbt parse --project-dir oxylabs --profiles-dir oxylabs
```

## Output

The main output is the `gold.gold_mrr_waterfall` model with:

- `month`
- `opening_mrr`
- `new_mrr`
- `reactivation_mrr`
- `expansion_mrr`
- `contraction_mrr`
- `churn_mrr`
- `closing_mrr`

## Current Scope

- The final mart focuses on MRR waterfall logic.
- `feature_usage`, `support_tickets`, and `churn_events` are loaded and staged, but not yet used in downstream marts.
- The project is designed for a local Postgres setup and is not yet incrementalized.
