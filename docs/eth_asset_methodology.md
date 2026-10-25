# ETH Asset Page — Data Methodology

Data behind the "ETH the asset" page, built by the Airflow DAG `api_eth_asset` (runs at `:35`, after the hourly CoinGecko price load). Only **two JSONs** are published:

| JSON | Updated | Sections (`data.<section>`) |
|---|---|---|
| `v1/eth_asset/live.json` | hourly | `kpis`, `stats`, `properties`, `productive_asset` |
| `v1/eth_asset/daily.json` | daily (07:35 UTC) | `purchasing_power`, `supply_flow`, `today`, `market_caps` |

| Section | Content |
|---|---|
| `kpis` | 5 KPI charts (timeseries per timeframe, current value, % change) |
| `stats` | Single values the frontend extrapolates per second (`total_wallets`, `eth_per_person`) |
| `properties` | "ETH is 6 things at once" section: yield breakdown, scarcity, collateral, ETH paid as gas |
| `productive_asset` | "ETH is a productive asset": yield comparison vs. central bank rates, dividends, gold, BTC, savings, and stake / lend / borrow |
| `purchasing_power` | "What one ETH actually buys": everyday goods per ETH today vs. a year ago, and what changed it |
| `supply_flow` | "The supply, in one picture": weekly issuance (tap) vs. burn (drain) for last week and historical scenarios |
| `today` | "Today in ETH": one AI-written card per day with a positive number from our data |
| `market_caps` | "How big is it really" section: current market cap of ETH vs. top cryptos, commodities and companies |

How the bundles are built: each section is created in its own DAG task, which returns the section dict (`{data, last_updated_utc}`, passed on via XCom, <100KB each) instead of uploading it. `run_create_live_bundle` and `run_create_daily_bundle` (trigger rule `all_done`) collect them and upload the bundle (`create_bundle_json`, sections in `ETH_ASSET_BUNDLES`). `sections_last_updated_utc` shows each section's freshness. A section that failed or was skipped keeps its data from the currently published bundle. The section headings below refer to these sections. Run locally (no S3 bucket), each section is also written to `output/v1/eth_asset/<section>.json`.

Caching: the bundles are uploaded **without CloudFront invalidations**. All of them use `Cache-Control: public, max-age=60, s-maxage=300, stale-while-revalidate=60, stale-if-error=86400` (`ETH_ASSET_CACHE_CONTROL`), so new data is visible on the API at the latest ~5 minutes after an upload. Logos are uploaded once and never invalidated.

Code: `backend/airflow/dags/api/api_eth_asset.py`, `backend/src/api/eth_asset_json.py`, `backend/eim/adapters/adapter_eth_supply.py`, `backend/eim/adapters/adapter_eth_collateral.py`, `backend/eim/adapters/adapter_reference_data.py`, `backend/src/db_connector.py` (`get_eth_burned`, `get_eth_staking_cl_daily`).

---

## 1. KPI charts (`live.json` → `kpis`)

### Timeframes

| Toggle | Bucket | Points | Built from |
|---|---|---|---|
| 24h | 1 hour | 24 | hourly data |
| 7d | 3 hours | 56 | hourly data |
| 60d | 1 day | 60 | daily data |
| 1y | 7 days | 52 | daily data |
| 5y | calendar month | 60 | daily data |

- Only complete hours / days are used. Hourly windows end at the last complete hour, daily windows at yesterday.
- 3h and 7d buckets are counted backwards from the window end, so every bucket is complete. Timestamps are bucket starts (unix ms, UTC).
- 5y uses calendar months; the current month is a partial bucket.

### Aggregation, current value and % change

| Type | KPIs | Bucket value | `current` | `change_pct` |
|---|---|---|---|---|
| Stock | price, supply, % staked | last value in bucket | latest hourly value (same for all toggles) | vs. value one window ago |
| Rate | staking yield | average over bucket | latest hourly value (same for all toggles) | vs. value one window ago |
| Flow | ETH burned | sum over bucket | total within the selected window | vs. total of the previous window of equal length |

- `change_pct` is a fraction (0.05 = +5%) and is `null` for 5y, or when data doesn't cover the comparison window.
- Yields and % staked are fractions as well (0.0256 = 2.56%).

### KPI definitions and sources

*Public attribution* (the `sources` field in the JSON): Dune is only our query infrastructure. Those queries and the logic are ours, so they are attributed to growthepie.

| KPI | Sources in JSON |
|---|---|
| ETH price | CoinGecko |
| ETH supply | ultrasound.money |
| Staking yield | ethsupply.fyi, growthepie |
| % of supply staked | ethsupply.fyi, growthepie, ultrasound.money |
| ETH burned | growthepie |
| Total wallets | growthepie |
| ETH per person | ultrasound.money, World Bank |
| Yield breakdown | ethsupply.fyi, growthepie |
| Yield comparison | ethsupply.fyi + growthepie (ETH), FRED (Fed, ECB, savings), Yahoo Finance (S&P 500 dividend) |
| Scarcity | ultrasound.money (ETH), World Gold Council (gold), FRED (US dollar) |
| Collateral | DefiLlama, growthepie |
| ETH paid as gas | growthepie |
| Market caps | CoinGecko (crypto), CompaniesMarketCap (companies, gold, silver, platinum), EIA via Our World in Data + Yahoo Finance (oil) |

**ETH price (USD)** — CoinGecko. Hourly in `fact_kpis_granular` (`price_usd`), daily in `fact_kpis`.

**ETH supply (ETH)** — ultrasound.money `supply-over-time` API. Daily from the `since_burn` series (`eim_fact.eth_supply_eth`, start-of-day snapshot), hourly from the `d30` series (last datapoint per hour).

**ETH burned (ETH)** — EIP-1559 base fee burn + EIP-4844 blob fee burn:
`Σ (base_fee_per_gas × gas_used + blob_base_fee × blob_gas_used) / 1e18`
- From Dencun (2024-03-14) onwards: growthepie's own Ethereum block data (`ethereum_blocks`), hourly and daily.
- 2021-08-05 (London) – 2024-03-13: one-off backfill from Dune `ethereum.blocks` (base fee burn only; no blobs before Dencun). Dune query 8951324.
- Cross-checked against ethsupply.fyi: 24h burn within 0.04%.

**% of supply staked** — staked ETH ÷ ETH supply.
Staked ETH = **actual balance** (not effective balance) of active validators. Pending deposits and validators in the activation queue are excluded.
- Hourly: ethsupply.fyi `/api/history?range=24h` (`activeBalanceGwei`, last datapoint per hour). Fallback if ethsupply.fyi is unavailable: Dune `beacon.epoch_summaries.active_balance` (query 8951203). This lags ~9h and is effective balance (~0.15% lower).
- Daily: Dune `beacon.validator_day_summaries`, sum of `start_balance` of active validators (start of day). Matches ethsupply.fyi active balance within 0.2% on every day of 2026.

**Staking yield** — annualized consensus-layer (CL) yield + execution-layer (EL) yield, shown as one aggregated value.

- *CL yield* = net CL rewards (issuance − consensus penalties) × 365 ÷ staked ETH. Sources by period:

  | Period | Source |
  |---|---|
  | 2020-12-01 – 2025-05-06 | Dune `beacon.validator_day_summaries.reward_change` (query 8951204) |
  | 2025-05-07 – 2026-01-01 | Theoretical issuance (see below) |
  | 2026-01-02 – now | ethsupply.fyi (`issuanceWei − consensusPenaltiesWei` per epoch), hourly, rolled up to complete days |

  Dune's `reward_change` is unreliable after Pectra (May 2025): ~70 days with spikes up to 68% APR or drops to ~0.7%, likely consolidations/deposits counted as rewards and incomplete days. It's therefore not used after 2025-05-06.

  *Theoretical fill:* the protocol issues `64 × √(total active balance in gwei)` gwei per epoch at full participation. This is multiplied by a participation factor and 225 epochs/day. The factor is linearly interpolated from 0.9754 (median Jan–May 2025, Dune) to 0.9846 (Jan 2026, ethsupply.fyi). The transitions are smooth: 2.763% → 2.770% on 2025-05-06/07, and 2.739% → 2.744% at the turn of the year. 2026-01-01 also uses the theoretical value because ethsupply.fyi's first interval only covers ~8h.

  Manual fix: 2023-04-15/16 (Shapella week). Dune split rewards unevenly across the day boundary; the two-day total is spread evenly over both days.

- *EL yield* = rewards received by block proposers × 365 ÷ staked ETH. Per block: the MEV-boost payment (last transaction, sent by the block's fee recipient/builder) if present, otherwise the priority fees. Source: Dune `ethereum.blocks` + `ethereum.transactions` (query 8951204), daily from the Merge (2022-09-15); 0 before. Large real spikes on MEV-heavy days (e.g. USDC depeg 2023-03-11, Oct 10 2025 liquidations).

- *Smoothing:*
  - Daily charts (60d, 1y, 5y): 7-day rolling average of daily CL + EL yield.
  - Hourly charts (24h, 7d): hourly CL yield (trailing 24h average) + 7-day average EL yield (EL is only available daily). If hourly CL data is missing, the 7-day average daily CL yield is used.

### Storage

| Data | Table | metric_key |
|---|---|---|
| Hourly | `fact_kpis_granular` (granularity `hourly`, origin_key `ethereum`) | `price_usd`, `eth_supply_eth`, `eth_burned_eth`, `eth_staked_eth`, `staking_rewards_cl_eth` |
| Daily | `eim_fact` (origin_key `ethereum`) | `eth_supply_eth`, `eth_burned_eth`, `eth_staked_eth`, `staking_rewards_cl_eth`, `staking_rewards_el_eth`, `staking_yield_cl`, `staking_yield_el` |
| Daily | `fact_kpis` | `price_usd` |

The daily Dune staking query only writes `eth_staked_eth`, `staking_rewards_el_eth` and `staking_yield_el`. CL values come from ethsupply.fyi.

---

## 2. Stats (`live.json` → `stats`)

Each value comes with a `timestamp` (unix ms) and a `per_second_increase`. The frontend shows `value + (now − timestamp) × per_second_increase`.

**Total wallets** — the number of unique addresses that ever initiated a transaction on Ethereum mainnet. This is the same number as the active addresses lifetime achievement on the Ethereum chain page (`select_total_aa.sql.j2`, HyperLogLog union over `fact_active_addresses_hll`, estimation error ~0.3%).
- `value`: all complete days, so `timestamp` = today 00:00 UTC.
- `per_second_increase`: average growth over the last 30 days. A 7-day window is noticeably noisier because of HLL estimation error.
- Source: growthepie.

**ETH per person** — ETH supply ÷ world population.
- Supply: latest hourly supply (ultrasound.money); `timestamp` = that hour.
- World population: World Bank Population Estimates and Projections (`SP.POP.TOTL`, source 40, mid-year values incl. projections), linearly interpolated between July 1 values.
- `per_second_increase` = S′/P − S·P′/P², where S′ is the average supply change per second over the last 7 days (hourly supply) and P′ is the population growth per second of the current interpolation segment.
- Values as of Oct 2026: ~0.0147 ETH/person, increase ~+2.7e-13 ETH/s. Supply grows ~2.9k ETH/day and population ~186k/day, so the per-second change only shows around the 13th decimal.
- `details` includes the underlying supply, population and their per-second rates.

---

## 3. Properties (`live.json` → `properties`)

**Yield breakdown**: the current staking yield split into issuance, tips and MEV. `total` is identical to `staking_yield.current` in `kpis`.
- `issuance`: CL yield, trailing 24h average of hourly ethsupply.fyi data (same as the hourly KPI).
- `tips` and `mev`: 7-day average of daily EL rewards ÷ staked ETH, split per block (Dune query 8951204, daily since the Merge):
  - Locally built block: the proposer receives the priority fees. All of it counts as `tips`.
  - MEV-boost block: the proposer receives the builder's payment. The payment first covers the block's priority fees (`tips`, capped at the payment); anything above that is `mev`.
  - Both parts are never negative and add up to the EL reward. Builders often pay less than the block's priority fees (they keep a margin), so `mev` is the smaller part. As of Oct 2026: tips 0.07%, MEV 0.01%.

**Scarcity**: annual change in the total stock of each asset, as a fraction.
- ETH: supply change over the last 30 days (latest hourly supply vs. the start-of-day supply 30 days ago), annualized. Source: ultrasound.money.
- Gold: World Gold Council above-ground stock. The figure ("Total above-ground stock (end-Q2 2026): 222,600 tonnes") is scraped daily at 06:00 UTC from gold.org by `AdapterReferenceData`. It's stored as `eim_fact.gold_stock_tonnes` (origin_key `gold`) at the end date of its period; WGC updates it quarterly. The annual change compares the latest stock with the latest stored value that is at least a year older (else the oldest), annualized. The year-end values for 2024 (216,265 t) and 2025 (219,891 t) were seeded once. Until mid-2027, the comparison is therefore 2026-06-30 vs. 2024-12-31 (+1.96%/yr); after that it is a clean year-over-year change. If the page layout changes, the scrape fails and the last stored value is used.
- US dollar: M2 money supply (FRED `M2SL`, monthly, seasonally adjusted), latest month vs. the same month a year earlier.

**Collateral**: ETH-equivalent tokens deposited in DeFi on Ethereum + L2s with ETH as gas token (chains via `aliases_defillama` in `main_config`). Daily snapshot from DefiLlama, converted to ETH with the latest hourly ETH price. Categories are partitioned so nothing is counted twice:
- `lending`: all ETH-equivalent tokens (ETH, WETH, LSTs, LRTs) in protocols of the DefiLlama category "Lending". This is DefiLlama TVL, i.e. supplied minus borrowed, which avoids inflating the number through looping.
- `dex`: all ETH-equivalent tokens in protocols of the category "Dexs".
- `liquid_staking`: ETH staked via protocols of the category "Liquid Staking" (Ethereum mainnet) **minus** the LST tokens already counted in lending and DEX. This is the liquid-staked ETH that is not deployed elsewhere.
- Only protocols with ≥ $5M TVL on the selected chains are fetched (35 lending, 25 DEX as of Oct 2026).
- Token classification: ETH/WETH = native. LSTs = the EIM `eth_derivatives` list plus wBETH, osETH, lsETH, ETHx and swETH. LRTs (ezETH, rsETH, rswETH, pufETH) count in lending/DEX but aren't subtracted from liquid staking.
- Known small overlap: LRTs hold LSTs underneath (e.g. rsETH backed by stETH), so about $0.9B of LRT collateral in lending is also partly inside the liquid staking total.
- Values as of 2026-10-15: 18.4M ETH ($43.6B) in total, 64% liquid staking, 34% lending, 1% DEX.
- Stored in `eim_fact`: `collateral_lending_eth`, `collateral_liquid_staking_eth`, `collateral_dex_eth`.

**ETH paid as gas**: daily sum of `fees_paid_eth` (and `fees_paid_usd`) from `fact_kpis` over all chains in `main_config` with `metadata_gas_token = 'ETH'`, `api_in_main` and deployment flag `PROD` (Ethereum + 17 L2s). `fees_paid_eth` exists for every chain, but for chains with another gas token it is only a currency conversion. Those chains are therefore excluded: Polygon PoS (POL), Mantle (MNT), Metis, Ronin, Gravity, Plume, Fraxtal, Celo (multiple gas tokens), and Starknet (ETH or STRK, which our data can't separate). The last 365 complete days are included. `chains` lists the chains with fees in the last 7 days.

---

## 4. Productive asset (`live.json` → `productive_asset`)

**Yield comparison** (`data.yield_comparison`): The nominal annual yield each asset pays just for holding it, as fractions. Fetched on every run; nothing is hardcoded except the zeros, which hold by definition.
- ETH staked: same value as `yield_breakdown.total` / `staking_yield.current` in `kpis`.
- US central bank rate: FRED `DFF`, the effective federal funds rate (daily). This is the rate banks actually pay overnight, and it moves within the Fed's target range (3.75–4.00% as of Oct 2026).
- ECB rate: FRED `ECBDFR`, the ECB deposit facility rate (daily), the ECB's main policy rate.
- S&P 500 dividend: SPY (S&P 500 ETF) dividends of the last 12 months ÷ current price (Yahoo Finance). This is close to the index yield; SPY's 0.09% expense ratio lowers it slightly.
- Gold, Bitcoin: 0. Neither pays anything for holding it.
- Cash savings: FRED `SNDR`, the FDIC national average rate on US savings deposits (monthly).

**Stake / Lend / Borrow** (`data.productive_uses`): Refreshed hourly. All rates are APRs (simple, not compounded) as fractions.
- **Stake**
  - `apr` and `apr_breakdown`: identical to `properties.yield_breakdown` (issuance, tips, MEV).
  - `total_staked_eth` and `share_of_supply`: the latest hourly values, the same as `staked_share` in `kpis`.
  - `active_validators`: the number of validators in the current epoch's beacon committees. Every active validator is in exactly one committee per epoch. This number has fallen since Pectra because validators are consolidating into balances of up to 2,048 ETH.
  - `exit_queue.days`: time from now until the latest exit epoch already assigned to an exiting validator. A new exit request would start there. After its exit epoch, a validator still waits about 27 hours (the withdrawability delay) plus the withdrawal sweep, and `days` does not include this.
  - `entry_queue.days`: ETH in pending deposits ÷ the churn limit. Since Electra, the churn limit is min(256, max(128, staked ÷ 65,536)) ETH per epoch, which is currently 57,600 ETH/day.
  - Source: a public beacon node via the standard beacon API (`publicnode.com`). If the node fails, these fields are `null` and a Discord alert is sent.
- **Lend**
  - The market is Aave v3 Ethereum (core). It is the largest ETH lending market: per DefiLlama in Oct 2026, it holds about 3× the ETH supplied to SparkLend, the next largest.
  - Values are read onchain from the Pool contract. `apr` is the WETH supply rate, which is paid entirely by borrowers: supply APR ≈ borrow APR × utilisation × (1 − reserve factor).
  - `market_available_liquidity_eth` = supplied − borrowed. This is the amount that can be withdrawn instantly.
  - `lending_supplied_eth`: all ETH in lending markets on Ethereum and ETH-gas L2s. It is the same number as `properties.collateral.breakdown.lending`.
- **Borrow**
  - `apr`: the stablecoin variable borrow rate on Aave v3 Ethereum. It averages USDC and USDT, weighted by the amount borrowed.
  - `apr_breakdown` follows Aave's rate curve:
    - `base_rate`: the rate at 0% utilisation. This is currently 0 for USDC and USDT.
    - `utilisation`: slope 1 × U ÷ U_optimal.
    - `above_kink_premium`: the steep slope 2, which applies only above optimal utilisation.

    The components are scaled to sum exactly to the onchain rate.
  - `max_ltv` and `liquidation_threshold`: from WETH's reserve configuration (no e-mode, since e-mode only applies to ETH-correlated borrows).
  - `eth_collateral_*`: ETH in lending markets, the same as `lend.lending_supplied_eth`.
  - Source: Ethereum RPC from our `sys_rpc_config` (the special-use RPC first, then the realtime RPCs).

---

## 5. Purchasing power (`daily.json` → `purchasing_power`)

How many units of everyday goods 1 ETH buys today vs. a year ago, for 4 regions (`data.regions`: `us`, `euro_area`, `uk`, `japan`). Rebuilt **once a day** (07:00 UTC). Each region is computed in its local currency, and all items use the **same unit in every region**:

| Item | Unit |
|---|---|
| Bread | 500 g loaf |
| Eggs | dozen |
| Coffee | 500 g, ground |
| Fuel | litre |
| Gold | gram |

- ETH price: today = latest complete hour (`fact_kpis_granular`, CoinGecko, USD); a year ago = daily `price_usd` on the same date one year earlier. Converted to local currency with FRED daily FX (`DEXUSEU`, `DEXUSUK`, `DEXJPUS`) at the respective date, so the ETH price effect includes currency moves.
- Gold: Yahoo Finance `GC=F` per troy ounce ÷ 31.1035 = per gram, converted with the same FX rates.
- Each item's price is compared with its price one year before its latest observation. Official data lags 1–2 months; `price_as_of` says which month "today" refers to.
- Source prices are converted to the standard unit (`source_unit` in the JSON shows the original):

| Region | Bread (→ 500 g) | Eggs (→ dozen) | Coffee (→ 500 g ground) | Fuel (→ litre) |
|---|---|---|---|---|
| US (USD) | BLS `APU0000702111`, white bread per lb × 500/453.6 | BLS `APU0000708111`, dozen | BLS `APU0000717311`, ground roast per lb × 500/453.6 | EIA `GASREGW`, per gallon ÷ 3.785 (weekly) |
| Euro area (EUR) | Anchor: INSEE baguette per kg (France, Jan 2025, `000442423`) × Eurostat HICP `CP01113` bread & bakery, × 0.5 | Anchor: €3.65 per dozen (Numbeo, average of DE/FR/IT/ES/NL, Aug 2026, **non-official**) × HICP `CP01148` eggs | Anchor: INSEE ground coffee 250 g (France, Dec 2019, `000442545`) × HICP `CP01220` coffee, × 2 | Anchor: INSEE unleaded 95 per litre (France, Jan 2025, `000849411`) × HICP `CP07222` petrol |
| UK (GBP) | Anchor: ONS white sliced 800 g (Jan 2025, `CZOH`) × ONS CPI `L78J` bread, × 500/800 | Anchor: ONS dozen (Jan 2025, `J428`) × CPI `L79A` eggs | Anchor: ONS ground coffee 227 g (Jan 2021, `CZNO`) × CPI `L7A6` coffee, × 500/227 | Anchor: ONS unleaded petrol per litre (Jan 2025, `CZMK`) × CPI `L7FP` petrol |
| Japan (JPY) | e-Stat Retail Price Survey, Tokyo 23 wards: 食パン per 1 kg × 0.5 | 鶏卵 pack of 10 × 1.2 | コーヒー豆 (ground regular coffee) per 100 g × 5 | Gasoline (regular) per litre |

- **Anchor method** (Euro area, UK): `price(month) = anchor price × index(month) / index(anchor month)`. The anchor is fetched once from an official series at a fixed month: INSEE for the euro area (France as the starting level), and the ONS average price series for the UK (discontinued after Jan 2025; ground coffee after Jan 2021). The euro-area egg anchor is the only hardcoded value, because no official euro price exists. Month-to-month movement always follows the official price index, so the "shop price" bar is official inflation for that item. Euro-area HICP has no separate bread index at euro-area level, so "bread and bakery products" is used.
- Japan units were verified against published survey tables: 食パン 1 kg, 鶏卵 pack of 10, コーヒー豆 100 g (ground regular coffee), gasoline 1 L.
- **Japan** needs the env var `ESTAT_APP_ID` (free e-Stat API key), locally and in Airflow. Table `0003421913`, area `13100`.
- ONS rate-limits quick successive requests. The code pauses between ONS calls and retries on HTTP 429.
- `change_units` splits the change in units exactly into two parts that add up to `units_per_eth − units_per_eth_year_ago`:
  - `eth_price` = ETH price change (in local currency) at year-ago shop prices: `eth_now / price_year_ago − eth_year_ago / price_year_ago`.
  - `shop_price` = shop price change (inflation) at today's ETH price: `eth_now / price_now − eth_now / price_year_ago`.

---

## 6. Supply flow (`daily.json` → `supply_flow`)

Inflation/burn explainer: weekly issuance to stakers (the tap) vs. burn (the drain). Rebuilt **once a day** (07:00 UTC), based on complete days only.

- Issued = net CL rewards (issuance − consensus penalties, ethsupply.fyi, daily rollup of hourly data). Burned = base fee + blob fee burn (own `ethereum_blocks`). Same daily data as the KPIs, from `eim_fact`.
- Scenarios (`data.scenarios`, in toggle order):
  - `last_week`: actual issuance and burn of the last 7 complete days, plus `daily` (issued, burned, supply at end of day) for the time-lapse.
  - `quiet_week`: the real 7-day window whose burn is closest to the 10th percentile of weekly burn over the last 365 days.
  - `busy_week`: the real 7-day window whose burn is closest to the 90th percentile of weekly burn since the Merge (Sep 15, 2022), the same PoS issuance regime as today. Over the last year alone, even busy weeks burn so little (<1k ETH) that they'd look like quiet weeks; since the Merge, a busy week roughly offsets issuance.
  - `mania`: the highest 7-day burn since EIP-1559 (Aug 2021), currently Jan 9–15, 2022 (~112k ETH).
  - All scenarios use **today's** weekly issuance (last 7 days). Issuance depends on the amount staked, not on network activity, so only burn changes between scenarios. Mania's original week was before the Merge, so only its burn is used.
- Per scenario: `issued_per_week`, `burned_per_week`, `net_per_week`, `net_annual_pct` (= net per week × 365/7 ÷ supply), `burned_share_of_issuance`, `period`, `description`.
- `demand_level` places each scenario on the "network demand" slider, on a log scale of weekly burn: quiet week = 0, mania = 1. `break_even_burn_per_week` (= weekly issuance) and `break_even_demand_level` mark where supply stops growing (currently ~20.8k ETH/week, level ~0.75).
- `supply`: hourly supply (ultrasound.money) at the end of the last complete day. The daily water levels of last week are reconstructed backwards from it with issued − burned, because the daily supply snapshots are not taken at consistent times.

---

## 7. Today in ETH (`daily.json` → `today`)

One new card per day (07:00 UTC) with one number, a headline and one sentence. Only **positive** ETH developments are shown. Code: `backend/src/api/eth_today_cards.py`, DAG task `run_create_today_card`.

**Principle:** numbers never come from the AI. Code computes candidate facts from our data; Gemini (`gemini-3.8-flash`, temperature 0.4, structured output) only picks one candidate and writes the text.

1. **Candidates** (daily, complete days):
   - ETH & staking: ETH price, net supply change (30d annualized), % staked, ETH staked, staking yield (7d avg), ETH burned per day, ETH/BTC market cap ratio.
   - Ethereum + L2s: ETH paid as gas (Ethereum + ETH-gas L2s), median L2 transaction cost (txcount-weighted median of the chains' medians, daily and 30d avg), transactions per day (daily and 30d avg).
   - Ethereum mainnet (`fact_kpis`, origin_key `ethereum`, history since 2015 where available): stablecoin supply (`stables_mcap`), throughput (`gas_per_second`), gas limit (`gas_limit`, an *event* metric: only a candidate if it moved ≥ 1% in the last 7 days), transactions (`txcount`), median transaction cost (`txcosts_median_usd`), daily active addresses (`daa`), active addresses over 30 days (`aa_last30d`), app fees (`app_fees_usd`). Contract deployments were left out on purpose: spikes are often automated mass deployments.
   - Mainnet extras: active apps per day (distinct `owner_project` in `vw_apps_contract_level_materialized`), stablecoin share of mainnet transactions (7d rolling, `blockspace_fact_category_level` stablecoin ÷ total_usage).
   - Milestones (only when a round number was crossed on the latest day): total wallets (every 1M; 5M/10M score higher), lifetime transactions (`lifetime_txcount`, every 100M; 500M multiples score higher), lifetime fees (`lifetime_fees_paid_eth`, every 10k ETH; 100k multiples score higher). Plus 7-day wallet growth (always positive, last resort).
2. **Facts per candidate**: value, 1/7/30-day changes, "highest/lowest since …" records (previous higher/lower value ≥ 60 days ago), and the typical 7-day change over the last year when the move is unusual. Dates are written like "Aug 19, 2026".
3. **Score**: how unusual the 7d/30d move is vs. the last year (z-score, capped at 4), plus a record bonus that only counts if the move behind it is in the same direction. Level metrics (ETH staked, 30d L2 cost, 30d transactions) get half the bonus for slow records in the good direction.
4. **Sentiment**: each metric has a good direction (up: price, % staked, ETH staked, burn, gas paid, transactions, ETH/BTC, stablecoin supply, throughput, gas limit, active addresses, app fees, active apps, stablecoin share; down: L2 and mainnet transaction cost, net supply; neutral: staking yield). A candidate is `positive` if its notable move or record points in the good direction (score × 1.5).
5. **Positive-only pool**: Gemini only sees positive candidates, and metrics used in the last 7 cards are removed. Order of preference: (1) notable positive candidates, (2) mild positives (a 30d trend in the good direction with at least a modest move: |z| ≥ 0.3, and ≥ 0.05 percentage points for % metrics) plus 7-day wallet growth, (3) only if nothing else is left, a repeat.
6. **Prompt**: pick the most interesting candidate, avoid metrics used in the last 7 days (it sees the last 14 cards), frame constructively but truthfully, use only the facts, numbers and dates as given, no hype, no causes or predictions.
7. **Validation**: the chosen candidate must be in the pool, headline ≤ 60 and sentence ≤ 200 characters, and every number in the text must appear in that candidate's facts (time windows 1/7/24/30/365 and "L2"/"Layer 2" are allowed). If the AI fails or is rejected, a plain template card from the top positive candidate is used (`model: fallback`).
8. **Output**: the value, unit and 30-day sparkline come from the candidate, never from the AI. Each card is stored in `public.eth_today_cards` (one row per day: date, candidate, headline, sentence, model, the AI's reasoning, full card as JSON). `today` serves the last 30 cards, newest first. Each new card is posted to Discord (`GTP_AI_WEBHOOK_URL`).

Env vars: `GEMINI_API_KEY`, `GTP_AI_WEBHOOK_URL`.

---

## 8. Market caps (`daily.json` → `market_caps`)

Current market cap in USD, rebuilt **once a day** (07:00 UTC). Nothing is hardcoded, and nothing needs manual updates.

- **Crypto**: ETH, BTC, USDT, USDC from CoinGecko `/coins/markets`.
- **Companies**: every company with a bigger market cap than ETH, plus the next 10 below ETH (currently 50 + 10), from the CompaniesMarketCap CSV (`companiesmarketcap.com/?download=csv`, already in USD). New entrants appear automatically.
- **Commodities**:
  - Gold, silver, platinum: every commodity on the CompaniesMarketCap assets ranking (rows whose logo id ends in `.XM`). Their method is estimated above-ground stock × spot price.
  - Oil, which CompaniesMarketCap doesn't list: world proved reserves (EIA via Our World in Data, latest year read at runtime, currently 2021: ~1,659B barrels) × Brent price (Yahoo Finance `BZ=F`). Most of this oil is still underground, which is why it dwarfs everything else (~$170T).
- **ETH/BTC market cap ratio** (`eth_btc_ratio`): daily timeseries for the last 2,000 complete days (~5.5 years), values as of 00:00 UTC. `current` is the live ratio from CoinGecko. Our CoinGecko plan only serves 2 years of history, so the series is built from:
  - ETH: `fact_kpis.market_cap_usd` (origin_key `ethereum`, collected daily from CoinGecko since 2015).
  - BTC: Yahoo Finance `BTC-USD` daily close of the previous day × circulating supply from blockchain.com (`total-bitcoins` chart, interpolated to daily). Checked against CoinGecko over 2 years: identical on average, 0.16% standard deviation per day.
- **Logos**: downloaded the first time an item appears and served from `api.growthepie.com/v1/eth_asset/logos/<key>.png`. Sources: CoinGecko for coins, CompaniesMarketCap (128px) for companies and metals, financialmodelingprep.com for oil. `key` is derived from the ticker (e.g. `nvda`, `2222_sr`), so a company that newly enters the list gets its logo on its first run.
- If parsing the CompaniesMarketCap assets page breaks (page layout change), the task fails instead of publishing wrong numbers, and the previous JSON stays live.

---

## 9. Source reliability and fallbacks

| Source | Used for | If unavailable |
|---|---|---|
| ethsupply.fyi | Hourly staked ETH, CL rewards | Staked ETH falls back to Dune (lagging, effective balance); CL yield falls back to 7-day average of daily values; Discord alert |
| ultrasound.money | Supply (hourly + daily) | Task fails/retries; JSON tasks don't run, so the previous JSONs stay live |
| Dune | Daily staked ETH, EL rewards, hourly staked fallback | Daily EL data retried on the next hourly run until yesterday is loaded |
| World Bank | Population (stats) | Stats task fails; `kpis` is unaffected |
| DefiLlama | Collateral | Collateral task fails/retries; previous `properties` section stays live |
| FRED | US dollar M2 (properties), Fed and ECB rates, savings rate (productive asset), US shop prices and FX (purchasing power) |
| Eurostat / INSEE / ONS / e-Stat | Regional shop prices (purchasing power) | Purchasing power task fails/retries; previous `purchasing_power` section stays live | Affected task fails/retries; previous JSON stays live |
| CoinGecko / CompaniesMarketCap / OWID / Yahoo Finance | Market caps | Market caps task fails/retries; previous `market_caps` section stays live |
| Gemini | Today card text | Falls back to a template card from the top positive candidate |
| gold.org (scrape) | Gold stock for scarcity | Reference data task fails; scarcity keeps using the last stored gold stock |
| Own `ethereum_blocks` | Burn | — |
