# Stablecoin balances adapter: status

As of 2026-10-27. On `main`: `metrics_stables_balances` (daily 01:45 UTC), the Airtable additions in `oli_airtable`, and the app metric `stables_balance` in the app JSONs (commit 308d3088, not yet deployed at the time of writing).

**Goal:** daily stablecoin balances, backfilled at least 3 months, for every address with an OLI `owner_project` plus the larger holders found by discovery, on every chain with app metrics, so balances can be attributed to applications and categories.

**Where it stands:** all 11 EVM app-metrics chains are measured daily (`origin_keys: ['*']`), with history from 2025-01-01 (backfilled manually; MegaETH from 2026-02-08, Robinhood from 2026-05-12, Unichain from 2025-01-21). All OLI-mapped addresses are included; small unlabeled candidates are pruned. Per-app stablecoin balances are aggregated daily into `vw_stables_balance_apps_materialized` and exposed in the app overview and app detail JSONs as `stables_balance`. Still open: Starknet, Polygon coverage, the frontend.

Architecture write-up with diagrams and trial results: https://claude.ai/code/artifact/ff7e1053-75ce-422b-bbe4-0682213a8eb6 (Claude Docs; Discovery and Measurement sections predate the cadence, OLI and pruning changes below).

---

## Coverage on 2026-10-20 (trial chains)

Coverage was last checked for the three trial chains; the other 8 chains have not been checked yet.

| Chain | Active candidates | Stablecoin supply | Held by tracked addresses | Held by OLI-labeled owners |
| --- | --- | --- | --- | --- |
| Ethereum | 25,750 (incl. all 21,206 OLI) | $161.3B | 79.7% | 42.2% |
| Polygon PoS | 8,151 | $2.87B | 58.6% | 15.5% |
| Celo | 1,359 | $0.15B | 89.8% | 6.7% |

- Polygon's gap is USDT0 (48%) and native USDC (46%). A one-time Dune seed over 6 yearly windows (16,966 addresses) only lifted it from 53% to ~60%. Blockscout's holder API would solve it, but Polygon is a "featured chain" that needs the Builder, Business or Pro plan.
- Most labeled value is exchanges, issuers' own contracts and bridges (Ethereum: binance $38.4B, makerdao $10.5B, okx $4.1B). **Decided 2026-10-21:** these count as apps; no separate issuer/CEX line for now.
- Labeling: the top 20 unlabeled holders per chain go to the Airtable `Unlabelled` table daily (see below). A one-off export of all unlabeled holders is in `backend/output/stables_unlabeled_addresses_2026-10-07.csv` (gitignored, 9,302 addresses).

---

## What exists

### Code (on `main`)

| File | Role |
| --- | --- |
| `backend/src/adapters/adapter_stables_balances.py` | `AdapterStablecoinBalances`: discovery, measurement, pruning |
| `backend/airflow/dags/metrics/metrics_stables_balances.py` | DAG, daily 01:45 UTC: `run_stables_balances` (`origin_keys: ['*']`, `days` = 21) >> `refresh_stables_balance_apps` |
| `backend/src/queries/postgres/stables/create_stables_balances.sql` | DDL: 2 tables + view (applied, incl. `last_full_scan` and `pruned_on`) |
| `backend/src/queries/postgres/stables/stables_balances_chain_summary.sql.j2` | Daily totals per chain vs supply |
| `backend/src/queries/postgres/stables/stables_balances_unmapped_top.sql.j2` | Unlabeled holders ranked by balance |
| `backend/src/queries/dune/stables_balances_holder_discovery.sql` | Saved Dune query 8915328: top receivers per token in a window (`days`, `offset_days`) |
| `backend/src/queries/dune/stables_balances_top_holders.sql` | Saved Dune query 8915430: all-time net transfers (fallback only) |
| `backend/src/queries/postgres/stables/create_vw_stables_balance_apps_materialized.sql` | DDL: per-app daily balance view (applied 2026-10-27, owner = Airflow role) |
| `backend/src/config.py` | App metric `stables_balance` in `gtp_metrics_new['apps']` |
| `backend/src/api/json_creation.py` | App overview columns, app detail merge, KPI card for `stables_balance` |
| `backend/src/db_connector.py` | `get_unlabelled_stables_holders`, `get_latest_stables_balances_usd` (Airtable) |
| `backend/airflow/dags/oli/oli_airtable.py` | `airtable_write_contracts`: adds top 20 stablecoin holders per chain and fills `stablecoin_balance` |
| `backend/src/currency_config.py` | Added GHS, BOB, PYG, UYU, VES (FX via the GitHub currency API fallback) |

### Tables

| Object | Holds |
| --- | --- |
| `sys_stables_balances_candidates` | One row per (origin_key, address): `sources`, discovery dates, `measured_from` / `measured_to`, `last_full_scan`, `pruned_on` |
| `fact_stables_balances` | End-of-day balance per (date, origin_key, token_id, address); non-zero rows only |
| `vw_fact_stables_balances_v1` | Balances + USD (price from `vw_fact_stables_mcap_v2`) + issuer + OLI owner_project / usage_category / main category |
| `vw_stables_balance_apps_materialized` | Per (owner_project, origin_key, date): `stables_balance_usd`, `stables_balance_eth`. Full rebuild daily (`REFRESH ... CONCURRENTLY`, ~1.5 min) with current cross-chain OLI labels; zero rows on days an app held nothing |

Our DB user can't `ALTER` these tables (not owner); schema changes must be run by you.

### How it works

**Discovery** (which addresses to measure; every address found stays a candidate):

| Source | What | Cadence | Cost |
| --- | --- | --- | --- |
| `activity` | Top 1,000 per ranking by gas / tx count / active addresses, last 30 days | Daily | Postgres |
| `oli` | **All** OLI addresses with an `owner_project` (no cap) | Daily | Postgres |
| `dune` | Top 1,000 receivers per token over the last 7 days, at least $100k received | Daily | ~1 Dune credit per chain |
| `blockscout` | Current top 1,000 holders per token with at least 1% of chain supply, at least $10k | First run per chain, then Mondays | Free (PRO key; Polygon gets HTTP 402) |
| `dune_history` | Same as `dune` over windows back N years; `history_window_days` (e.g. 91 = quarterly) and `history_token_ids` to target the biggest gaps | Manual one-time seed (ran for Polygon, Base, Arbitrum) | ~2–4 min medium engine per window |
| `dune_holders` | All-time net transfers per token | Manual fallback only | ~60 credits on Ethereum; times out on Polygon and Celo |

**Measurement:** `balanceOf()` via Multicall3 at the last block of each day (`first_block_of_day(D+1) - 1`). Up to 1,000 balances per call, 6 calls in parallel, halving automatically when RPCs reject a batch (Polygon settles at 500). Each address gets a full scan of all mapped tokens every 7 days and only its held pairs in between; newly funded pairs found by a full scan are backfilled since the previous full scan. Historical backfills (dates before `measured_from`) use the same split in reverse (ad852d76). Addresses in `exclude_balances` of the stables config are never measured.

**Pruning** (after each run): candidates with no OLI `owner_project`, measured at least 14 days, and below $10k total on every day of the last 30 get `pruned_on` set and are no longer measured (history stays). A pruned address found again by a balance-based source (`dune`, `blockscout`, `dune_history`, `dune_holders`) or OLI is unpruned, gets a forced full scan, and the skipped days are backfilled. On 2026-10-21: 2,296 pruned on Ethereum, 17,196 on Polygon, 1,325 on Celo.

**RPCs:** non-special RPCs from `sys_rpc_config` only (active first, then inactive as fallback); paid `special_use` RPCs are never called. Failing RPCs move to the back of the list.

**Chains:** `origin_keys: ['*']` resolves to every chain with `runs_aggregate_apps` in `main_config`, minus non-EVM chains (Starknet). The DAG uses `['*']`: Arbitrum, Base, Celo, Ethereum, MegaETH, Mode, Optimism, Polygon PoS, Robinhood, Scroll, Unichain.

**App metric `stables_balance` (API JSONs, built by `api_json_creation` at 05:30 UTC):**
- `apps/app_overview_{1d,7d,30d,90d,365d,max}.json`: per (app, chain) row, appended columns `stables_balance_usd` (latest measured day), `prev_stables_balance_usd` (one timeframe earlier; 0 for `max`) and `stables_balance_eth`. 0 when an app has no stablecoin data (avoids NaN alerts).
- `apps/details/{project}.json`: metric `stables_balance` ("Stablecoin Balance") with daily `['unix','usd','eth']` series per chain, `aggregated` = average daily balance per timeframe (`monthly_agg: avg`), and `kpi_cards.stables_balance`. Chains without stablecoin data: empty series, `[null, null]` aggregates.
- Attribution: same cross-chain OLI labels as the other app metrics; exchanges and issuers' own contracts count as apps.

**Airtable (`oli_airtable` DAG, `airtable_write_contracts`):** the top 20 unlabeled stablecoin holders per chain (latest measured day) join the daily `Unlabelled` refill, with the reason in `_comment` ("Stablecoins held: $X (tokens)"). A `stablecoin_balance` column (USD, created 2026-10-21) is filled for every written row we have balance data for.

### Cost observed (2026-10-21 runs, 21-day window)

| Chain | RPC calls | Time | Notes |
| --- | --- | --- | --- |
| Ethereum | 10,714 | 58 min | Includes first full scans of 18,952 new OLI addresses (2.33M balance reads each) |
| Polygon PoS | 4,772 | 39 min | Includes the `dune_history` seed (6 Dune windows) |
| Celo | 325 | 2 min | |

A sparse day afterwards takes under a minute per chain. Full scans are CPU-heavy in Python (encoding ~1M Multicall entries takes several minutes).

---

## Open issues

1. **Polygon coverage ~59%** on the last check (see above); the quarterly `dune_history` seed may have improved it. Blockscout's Builder plan would close the gap.
2. **Coverage not yet checked for the 8 newer chains.** Run `stables_balances_chain_summary.sql.j2` for all chains.
3. **Starknet** is an app-metrics chain but isn't measured (non-EVM, no Multicall3); its app pages show empty `stables_balance` series.
4. **Frontend:** the JSON fields exist once 308d3088 is deployed; pages and the overview still need to display them.
5. **Local tunnel:** the VS Code port forward for `localhost:5432` dropped repeatedly (hung logins, `SSL SYSCALL error`). Dead clients leave server sessions `idle in transaction` that block matview `REFRESH` and `DROP` (happened twice). Suggested: `ALTER ROLE orbal_writer SET idle_in_transaction_session_timeout = '30min'` (DBA, check Airflow first), and a plain `ssh -L` tunnel for long runs.
6. **DB connections:** many `DbConnector` readers use `pd.read_sql(..., self.engine.connect())` without closing. A long-lived process (here: a stopped local run) kept transactions open on `vw_oli_label_pool_gold_v2` and blocked its production `REFRESH` for ~10 minutes on 2026-10-21. Fixed in ff931ba9 ("close connections used by pd.read_sql"); after interrupting local runs, still check `pg_stat_activity`.
7. **Housekeeping:** rotate the Blockscout key (it appeared in a log once; errors are redacted now); flag paid keyed RPCs in `sys_rpc_config` (Alchemy, Tatum, QuickNode, RPCfast rows) as `special_use`; backfill FX rates for the 5 added currencies so their stablecoins get USD values in past days.

---

## Next steps toward the goal

1. **Deploy 308d3088** and check the first runs: `refresh_stables_balance_apps` in `metrics_stables_balances`, then `api_json_creation` (no NaN alerts, `stables_balance` in overview and details).
2. **Frontend:** show `stables_balance` on the app overview (column + % change) and app pages (chart, KPI card).
3. **Check coverage on all 11 chains** and seed the weakest with `dune_history` (quarterly windows, top tokens).
4. **Polygon:** decide on the Blockscout plan upgrade.
5. **Starknet:** separate balance path (no Multicall3).
6. **Chain-level metric (optional):** share of a chain's stablecoin supply held by apps, from the same view.

---

## Runbook

```python
# from backend/, with the DB tunnel open
from dotenv import load_dotenv; load_dotenv('.env')
from src.db_connector import DbConnector
from src.adapters.adapter_stables_balances import AdapterStablecoinBalances
ad = AdapterStablecoinBalances({}, DbConnector())

# daily run as the DAG does it (one chain)
ad.extract({'origin_keys': ['celo'], 'days': 21})

# new chain: first run (discovery incl. blockscout, full scan, backfill)
ad.extract({'origin_keys': ['base'], 'days': 21})

# one-time seed for a chain without Blockscout access
ad.extract({'origin_keys': ['polygon_pos'], 'days': 21, 'sources': ['dune_history'], 'history_years': 6})

# measure only, no discovery / no pruning
ad.extract({'origin_keys': ['ethereum'], 'days': 21, 'discover': False, 'prune': False})
print(ad.rpc_calls)
```

Coverage check: render `stables_balances_chain_summary.sql.j2` with `origin_keys` and `days`, or query `vw_fact_stables_balances_v1` directly.

### Things that bit during the trial

- The DB tunnel on `localhost:5432` dropped several times; runs fail with `SSL SYSCALL error: EOF detected`. Progress is saved per day, so rerunning resumes.
- Stopping a local run does not stop a Dune execution already submitted. Cancel it via `POST https://api.dune.com/api/v1/execution/<id>/cancel`.
- Stopped local processes can leave `idle in transaction` sessions that block matview refreshes and drops; check `pg_stat_activity` (client_addr `127.0.0.1` = tunnel) after interrupting runs.
- Local JSON generation (`JSONCreation(None, None, ...)`) still sends Discord NaN alerts from `fix_dict_nan`; patch it to `send_notification=False` for local runs. `run_app_details_jsons_all` ends with a CloudFront invalidation; call `run_app_details_jsons(projects, is_all=True)` locally instead.
- Dune executions stop at 30 minutes; all-time scans of USDC/USDT on Polygon and Celo hit that limit.
- Two runs on the same chain at once duplicate RPC work (results stay correct, upserts are idempotent).
- Public RPCs: Polygon caps eth_call responses at 100 KB (~500 balances); many Ethereum/Polygon public nodes are pruned and fail on historical blocks.
- `exclude_balances` addresses (e.g. Tether treasury, Polygon USDC.e's $501M) must not be measured, or coverage exceeds 100% for that token.
