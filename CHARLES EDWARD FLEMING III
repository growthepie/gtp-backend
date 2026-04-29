# Welcome to the growthepie 📏🥧 Backend!

[growthepie](https://growthepie.xyz/) improves transparency across the Ethereum ecosystem by curating onchain, offchain, and community-sourced signals into actionable metrics, dashboards, and research.

<p align="center">
  <img src="https://github.com/growthepie/.github/assets/90760534/ca2ca39f-657b-4f79-8550-242b4ee9c4ec" alt="growthepie logo"/>
</p>

## What This Repo Does

The [`gtp-backend`](https://github.com/growthepie/gtp-backend) repository powers growthepie’s public dashboard and data products. It runs on Python **3.10.12** and houses:

- **Data ingestion adapters** that pull metrics from RPC nodes, Dune, CoinGecko, L2Beat, DeFiLlama, BigQuery, and bespoke indexers.
- **Curation and enrichment jobs** that normalize raw payloads, stitch cross-source identifiers, and enforce common schemas before persisting to PostgreSQL.
- **Analytics layers** that transform curated tables into pre-aggregated highlights, trendlines, and API payloads used by the dashboard, alerts, and JSON exports.
- **Airflow orchestration** for scheduling hourly/daily DAGs, data quality checks, and downstream notifications.

## Repository Highlights

- `backend/src/adapters/`: Source-specific connectors (`adapter_stables.py`, `adapter_defillama.py`, etc.) plus shared clients like `bigquery.py`.
- `backend/src/queries/`: Jinja-templated SQL powering metrics (e.g., `select_txcount.sql.j2`) and API endpoints.
- `backend/airflow/dags/`: Production DAGs grouped by feature area (`metrics_*`, `api_*`, `other_*`) with alerting and JSON generation pipelines.
- `backend/src/api/`: JSON builders (`json_gen.py`, `json_creation.py`) and the lightweight API surface in `oli/api`.
- `backend/src/config.py` and friends manage environment-specific wiring.

## Functional Flows

1. **Ingest** – Adapters fetch fresh slices of chain activity, market data, or third-party metrics.
2. **Transform** – Helper modules clean, reconcile, and upsert into curated PostgreSQL tables.
3. **Analyze** – SQL templates and Python processors compute KPIs for quantities like TPS, fees, security, and ecosystem health.
4. **Distribute** – Airflow DAGs trigger JSON generation, API refreshes, alerts, and highlights consumed by growthepie.com and partner feeds.

## Related Repositories

Explore the rest of the [`growthepie org`](https://github.com/growthepie) for front-end, infra, and research companions.
