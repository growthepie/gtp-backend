# Stablecoin balances adapter: status

As of 2026-10-21. Deployed: `metrics_stables_balances` (daily 01:45 UTC) and the Airtable additions in `oli_airtable` are on `main`.

**Goal:** daily stablecoin balances, backfilled at least 3 months, for every address with an OLI `owner_project` plus the larger holders found by discovery, on every chain with app metrics, so balances can be attributed to applications and categories.

**Where it stands:** Ethereum, Polygon PoS and Celo are measured daily with history from 2026-09-30 (21 days). All OLI-mapped addresses are included; small unlabeled candidates are pruned. Still missing: the 3-month backfill, the other app-metrics chains, and the aggregated metric for the API/site.

Architecture write-up with diagrams and trial results: https://claude.ai/code/artifact/ff7e1053-75ce-422b-bbe4-0682213a8eb6 (Claude Docs; Discovery and Measurement sections predate the cadence, OLI and pruning changes below).

---

## Coverage on 2026-10-20

| Chain | Active candidates | Stablecoin supply | Held by tracked addresses | Held by OLI-labeled owners |
| --- | --- | --- | --- | --- |
| Ethereum | 25,750 (incl. all 21,206 OLI) | $161.3B | 79.7% | 42.2% |
| Polygon PoS | 8,151 | $2.87B | 58.6% | 15.5% |
| Celo | 1,359 | $0.15B | 89.8% | 6.7% |

- Polygon's gap is USDT0 (48%) and native USDC (46%). A one-time Dune seed over 6 yearly windows (16,966 addresses) only lifted it from 53% to ~60%. Blockscout's holder API would solve it, but Polygon is a "featured chain" that needs the Builder, Business or Pro plan.
- Most labeled value is exchanges, issuers' own contracts and bridges (Ethereum: binance $38.4B, makerdao $10.5B, okx $4.1B). **Decided 2026-10-21:** these count as "held by apps"; no separate issuer/CEX line for now.
- Labeling: the top 20 unlabeled holders per chain go to the Airtable `Unlabelled` table daily (see below). A one-off export of all unlabeled holders is in `backend/output/stables_unlabeled_addresses_2026-10-07.csv` (gitignored, 9,302 addresses).

---

## What exists

### Code (on `main`)

| File | Role |
| --- | --- |
| `backend/src/adapters/adapter_stables_balances.py` | `AdapterStablecoinBalances`: discovery, measurement, pruning |
| `backend/airflow/dags/metrics/metrics_stables_balances.py` | DAG, daily 01:45 UTC, `origin_keys` = ethereum, polygon_pos, celo, `days` = 21 |
| `backend/src/queries/postgres/stables/create_stables_balances.sql` | DDL: 2 tables + view (applied, incl. `last_full_scan` and `pruned_on`) |
| `backend/src/queries/postgres/stables/stables_balances_chain_summary.sql.j2` | Daily totals per chain vs supply |
| `backend/src/queries/postgres/stables/stables_balances_unmapped_top.sql.j2` | Unlabeled holders ranked by balance |
| `backend/src/queries/dune/stables_balances_holder_discovery.sql` | Saved Dune query 8915328: top receivers per token in a window (`days`, `offset_days`) |
| `backend/src/queries/dune/stables_balances_top_holders.sql` | Saved Dune query 8915430: all-time net transfers (fallback only) |
| `backend/src/db_connector.py` | `get_unlabelled_stables_holders`, `get_latest_stables_balances_usd` (Airtable) |
| `backend/airflow/dags/oli/oli_airtable.py` | `airtable_write_contracts`: adds top 20 stablecoin holders per chain and fills `stablecoin_balance` |
| `backend/src/currency_config.py` | Added GHS, BOB, PYG, UYU, VES (FX via the GitHub currency API fallback) |

### Tables

| Object | Holds |
| --- | --- |
| `sys_stables_balances_candidates` | One row per (origin_key, address): `sources`, discovery dates, `measured_from` / `measured_to`, `last_full_scan`, `pruned_on` |
| `fact_stables_balances` | End-of-day balance per (date, origin_key, token_id, address); non-zero rows only |
| `vw_fact_stables_balances_v1` | Balances + USD (price from `vw_fact_stables_mcap_v2`) + issuer + OLI owner_project / usage_category / main category |

Our DB user can't `ALTER` these tables (not owner); schema changes must be run by you.

### How it works

**Discovery** (which addresses to measure; every address found stays a candidate):

| Source | What | Cadence | Cost |
| --- | --- | --- | --- |
| `activity` | Top 1,000 per ranking by gas / tx count / active addresses, last 30 days | Daily | Postgres |
| `oli` | **All** OLI addresses with an `owner_project` (no cap) | Daily | Postgres |
| `dune` | Top 1,000 receivers per token over the last 7 days, at least $100k received | Daily | ~1 Dune credit per chain |
| `blockscout` | Current top 1,000 holders per token with at least 1% of chain supply, at least $10k | First run per chain, then Mondays | Free (PRO key; Polygon gets HTTP 402) |
| `dune_history` | Same as `dune` over yearly windows back 6 years | Manual one-time seed (ran for Polygon) | ~2–4 min medium engine per window |
| `dune_holders` | All-time net transfers per token | Manual fallback only | ~60 credits on Ethereum; times out on Polygon and Celo |

**Measurement:** `balanceOf()` via Multicall3 at the last block of each day (`first_block_of_day(D+1) - 1`). Up to 1,000 balances per call, 6 calls in parallel, halving automatically when RPCs reject a batch (Polygon settles at 500). Each address gets a full scan of all mapped tokens every 7 days and only its held pairs in between; newly funded pairs found by a full scan are backfilled since the previous full scan. Addresses in `exclude_balances` of the stables config are never measured.

**Pruning** (after each run): candidates with no OLI `owner_project`, measured at least 14 days, and below $10k total on every day of the last 30 get `pruned_on` set and are no longer measured (history stays). A pruned address found again by a balance-based source (`dune`, `blockscout`, `dune_history`, `dune_holders`) or OLI is unpruned, gets a forced full scan, and the skipped days are backfilled. On 2026-10-21: 2,296 pruned on Ethereum, 17,196 on Polygon, 1,325 on Celo.

**RPCs:** non-special RPCs from `sys_rpc_config` only (active first, then inactive as fallback); paid `special_use` RPCs are never called. Failing RPCs move to the back of the list.

**Chains:** `origin_keys: ['*']` resolves to every chain with `runs_aggregate_apps` in `main_config`, minus non-EVM chains (Starknet). The DAG still lists the three trial chains explicitly.

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

1. **Backward backfill reads every token for every address.** Dates before an address's `measured_from` are always full scans: ~3,200 calls per day of history on Ethereum. A 90-day backfill would be ~90k calls. This is why the DAG runs `days: 21` for now.
2. **Archive depth of free RPCs is unverified beyond 21 days.** On Ethereum, drpc, tenderly, sentio and mevblocker served historical state; pocket, publicnode, blockpi and several others are pruned. Polygon: drpc and Tenderly's gateway work.
3. **Polygon coverage ~59%** (see above).
4. **No aggregated metric yet.** Balances per address with app/category attribution exist in the view, but nothing aggregates them into app or chain metrics for the API/site.
5. **Only 3 of 11 EVM app-metrics chains.** Arbitrum, Base, Optimism, Scroll, Unichain, MegaETH, Mode and Robinhood all have Multicall3, free RPCs and a Dune alias; Blockscout plan coverage is unchecked. Starknet needs its own path.
6. **DB connections:** many `DbConnector` readers use `pd.read_sql(..., self.engine.connect())` without closing. A long-lived process (here: a stopped local run) kept transactions open on `vw_oli_label_pool_gold_v2` and blocked its production `REFRESH` for ~10 minutes on 2026-10-21. Fixed in ff931ba9 ("close connections used by pd.read_sql"); after interrupting local runs, still check `pg_stat_activity`.
7. **Housekeeping:** rotate the Blockscout key (it appeared in a log once; errors are redacted now); flag paid keyed RPCs in `sys_rpc_config` (Alchemy, Tatum, QuickNode, RPCfast rows) as `special_use`; backfill FX rates for the 5 added currencies so their stablecoins get USD values in past days.

---

## Next steps toward the goal

1. **Check the first deployed runs** of `metrics_stables_balances` and `oli_airtable` (runtime, errors, Airtable rows with `stablecoin_balance`).
2. **Make backward backfill sparse.** Reuse the forward logic in the backward pass: full scan on the newest backfill day, then only held pairs, with a full scan every 7 days going back. That brings a 90-day Ethereum backfill to roughly 15k calls.
3. **Verify 90-day archive access** on each chain's free RPCs.
4. **Backfill to 90 days**, then raise the DAG's `days` (it only measures missing days per address, so a larger window costs nothing once filled).
5. **Build the metric:** daily aggregation of `vw_fact_stables_balances_v1` into app and chain metrics (by app, by category, share of chain supply in apps). Issuers' own contracts and exchanges count as apps.
6. **Roll out to the other app-metrics chains** a few at a time (`origin_keys: ['*']` when all are in), watching first-run cost; then Starknet.
7. **Polygon:** decide on the Blockscout plan upgrade.

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
- Stopped local processes can leave `idle in transaction` sessions that block matview refreshes; check `pg_stat_activity` (client_addr `127.0.0.1` = tunnel) after interrupting runs.
- Dune executions stop at 30 minutes; all-time scans of USDC/USDT on Polygon and Celo hit that limit.
- Two runs on the same chain at once duplicate RPC work (results stay correct, upserts are idempotent).
- Public RPCs: Polygon caps eth_call responses at 100 KB (~500 balances); many Ethereum/Polygon public nodes are pruned and fail on historical blocks.
- `exclude_balances` addresses (e.g. Tether treasury, Polygon USDC.e's $501M) must not be measured, or coverage exceeds 100% for that token.
