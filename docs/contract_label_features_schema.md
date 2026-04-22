# Contract Label Features & Review — Database Schema

This document describes the two-table design for capturing AI contract classification signals
and human review corrections. The intended use is training ML models, evaluating classifier
performance, and building ground truth for supervised learning.

---

## Overview: Two-Table Design

| Table | Purpose | Written by | Primary key |
|---|---|---|---|
| `contract_label_features` | Feature store — all signals computed per run + AI output | `automated_labeler.py` | `(address, origin_key)` |
| `contract_label_review` | Review audit log — diff between AI output and GTP-attested result | `oli_airtable.py` DAG | `id` (serial) |

**ML join pattern:**
```sql
SELECT f.*, r.gtp_usage_category, r.gtp_owner_project, r.name_was_changed, r.category_was_changed
FROM public.contract_label_features f
LEFT JOIN public.contract_label_review r USING (address, origin_key)
```

---

## Pipeline Context

### Attester addresses
| Address | Role | Trust level |
|---|---|---|
| `0xaDbf2b56995b57525Aa9a45df19091a2C5a2A970` | Automated labeler — general | Low (AI-generated) |
| `0xab81887E560e7C7b5993cE45D1c584d7FC22e898` | Automated labeler — trading specialist | Low (AI-generated) |
| `0xA725646C05E6BB813D98C5ABB4E72DF4BCF00B56` | GTP main attester (re-attestation key) | High (human-approved) |

### Approval flows that generate `contract_label_review` rows

| Airtable table | `review_source` value | What human does |
|---|---|---|
| "Label Pool Automated" | `airtable_automated` | Approves/edits `contract_name` and `usage_category`; optionally sets `gtp_owner_project` or checks `gtp_no_owner_project` |
| "Label Pool Reattest" | `airtable_reattest` | Assigns `owner_project` for protocol-likely contracts; optionally checks `gtp_no_owner_project` |
| "Unlabelled" | `airtable_unlabelled` | Manual labeling — no AI output to diff against |

---

## Table 1: `contract_label_features`

One row per contract per chain. Upserted on every labeling run (`ON CONFLICT (address, origin_key) DO UPDATE`).
Written by `automated_labeler.py` BEFORE OLI attestation, so even contracts below the 0.5 confidence
threshold are captured (valuable training data for hard cases).

> **TODO:** Table does not exist yet. DDL at bottom of this document.  
> **TODO:** `automated_labeler.py` does not write to this table yet. See `TODO(contract_label_features)` comment at line ~895.  
> **TODO:** `ai_classifier.py` does not return pre-computed signals yet. See `TODO(contract_label_features)` comment at line ~994.

### 1.1 Identity

| Column | Type | Source | Description |
|---|---|---|---|
| `address` | `bytea` | `blockspace_fact_contract_level.address` | Contract address (PK) |
| `origin_key` | `varchar` | pipeline | Chain identifier, e.g. `base`, `optimism` (PK) |
| `labeled_at` | `timestamptz` | pipeline | When this labeling run completed |
| `blockscout_name` | `varchar` | `BlockscoutAPI_Async.get_contract_info()` | Raw name from Blockscout (may be generic proxy name — see `is_proxy`) |
| `is_verified` | `bool` | Blockscout | Source code verified on Blockscout |
| `is_proxy` | `bool` | Blockscout | Contract is a proxy pattern (ERC-1967, Transparent, UUPS, etc.) |
| `impl_address` | `bytea` | Blockscout `implementations[]` | Implementation address (null if not proxy) |
| `impl_name` | `varchar` | Blockscout implementation lookup | Resolved implementation contract name |
| `impl_verified` | `bool` | Blockscout | Implementation source verified |
| `has_github_repo` | `bool` | `GitHubAPI_Async.search_contract_address()` | Contract address found in a public GitHub repo |
| `github_repo_count` | `int` | GitHub | Number of repos referencing this address |

### 1.2 Activity Metrics

Aggregated from `blockspace_fact_contract_level` over `metric_day_range` days.
Computed in `db_connector.get_unlabelled_contracts_v2()`.

| Column | Type | Description |
|---|---|---|
| `metric_day_range` | `int` | Window used for aggregation (default 7) |
| `txcount` | `int` | Total transactions in window |
| `gas_eth` | `numeric` | Total gas fees in ETH |
| `avg_daa` | `numeric` | Average daily active addresses |
| `avg_gas_per_tx` | `numeric` | `gas_eth / txcount` |
| `rel_cost` | `numeric` | Contract median tx fee / chain median tx fee (1.0 = at chain median) |
| `success_rate` | `numeric` | Fraction of successful transactions (0.0–1.0); null if unavailable |
| `daa_ratio` | `numeric` | `avg_daa / txcount × 100` — proxy for human use vs automation |

### 1.3 Caller Diversity

Derived from sampled Tenderly traces inside `classify_contract()`.

| Column | Type | Description |
|---|---|---|
| `caller_diversity_pct` | `numeric` | Unique caller addresses / sampled txns × 100 |
| `caller_diversity_label` | `varchar` | `PRIVATE` (≤20%) / `KEEPER` (20–80%) / `PUBLIC` (≥80% + DAA threshold) / `UNKNOWN` |
| `unique_callers` | `int` | Distinct sender addresses in trace sample |
| `total_traces_sampled` | `int` | Number of transactions sampled (context for diversity %) |

### 1.4 Trace-Derived Protocol Signals

Computed from Tenderly traces at depth-0/1 direct calls inside `classify_contract()`.
`novel_tokens` / `common_tokens` also augmented from Blockscout token-transfer endpoint.

| Column | Type | Description |
|---|---|---|
| `novel_tokens` | `text[]` | ERC20 names seen in ≥50% of traces, NOT in common-infra list (WETH/USDC/etc.) — HIGH signal for protocol identity |
| `common_tokens` | `text[]` | ERC20 names seen in ≥50% of traces that ARE common infra — low signal |
| `bs_token_transfers` | `jsonb` | Blockscout token-transfer records: `[{token_name, token_symbol, token_type}]` — more reliable than trace-decoded names |
| `matched_routers` | `text[]` | Named DEX routers called directly, e.g. `UniswapV3Router` |
| `matched_dex_pools` | `text[]` | AMM pool contracts called directly |
| `calls_into_dex_pool_swap` | `bool` | Made state-changing swap calls into AMM pools |
| `matched_lending` | `text[]` | Lending protocols called (Aave, Compound, Morpho, etc.) |
| `matched_staking` | `text[]` | Staking/gauge contracts called (Gauge, Voter, MasterChef, etc.) |
| `matched_bridges` | `text[]` | Bridge protocols called (Relay, LayerZero, Stargate, etc.) |
| `matched_oracle_fns` | `text[]` | Oracle function calls detected at any depth (latestAnswer, getPrice, etc.) |
| `delegates_to` | `varchar` | Named contract this contract delegates to (EIP-1167 target name) |
| `raw_delegate` | `bool` | EIP-1167 minimal proxy / clone (raw address delegate, no name) |
| `all_traces_empty` | `bool` | All sampled traces had no call graph (contract may only receive ETH) |
| `traces_only_raw_opcodes` | `bool` | Traces had no decoded function or contract names (unverified + unrecognized ABI) |
| `named_contracts_in_traces` | `text[]` | All named contracts seen anywhere in traces (any depth) |

### 1.5 Log Signals

Decoded from the contract's own emitted events via `BlockscoutAPI_Async.get_address_logs()` +
`decode_log_signals()` in `ai_classifier.py`.

| Column | Type | Description |
|---|---|---|
| `log_has_access_control` | `bool` | RoleGranted / Revoked / OwnershipTransferred events |
| `log_has_token_transfers` | `bool` | ERC20/721 Transfer or Approval events |
| `log_has_swaps` | `bool` | DEX Swap events |
| `log_has_bridge_events` | `bool` | MessagePassed / SentMessage / TransactionDeposited |
| `log_has_erc4337` | `bool` | UserOperationEvent / AccountDeployed (account abstraction) |
| `log_has_staking` | `bool` | Staking-related events |
| `log_has_governance` | `bool` | ProposalCreated / VoteCast |
| `log_category_hints` | `text[]` | Category signals derived from event patterns |
| `log_top_events` | `jsonb` | Event name → occurrence count, e.g. `{"Swap": 41, "Transfer": 12}` |

### 1.6 AI Output

The label produced by `classify_contract()` in `ai_classifier.py` (Gemini model).

| Column | Type | Description |
|---|---|---|
| `ai_contract_name` | `varchar` | Assigned human-readable name (max 50 chars) |
| `ai_usage_category` | `varchar` | Assigned OLI category ID, e.g. `dex`, `token_contract`, `trading` |
| `ai_confidence` | `numeric` | Model confidence 0.0–1.0 |
| `ai_reasoning` | `text` | Classifier reasoning string (`PATH X[subpath]: signal=val → category`) |
| `ai_model` | `varchar` | Model version, e.g. `gemini-2.5-flash` |
| `ai_owner_project_likely` | `bool` | `_is_protocol_likely()` sentinel result — TRUE means the contract likely belongs to a named protocol and should be surfaced for `owner_project` assignment |
| `sentinel_fired` | `bool` | TRUE if the rule-based sentinel short-circuited the AI call |
| `sentinel_category` | `varchar` | Category assigned by sentinel (null if AI ran normally) |

---

## Table 2: `contract_label_review`

Append-only audit log. One row per approval event. A contract can have multiple review rows
(re-labeled across weeks as new signals emerge or the label is corrected).

Written by `oli_airtable.py` tasks immediately before `oli.submit_label()` with `OLI_gtp_pk`.

> **TODO:** Table does not exist yet. DDL at bottom of this document.  
> **TODO:** `airtable_read_automated_labels()` does not write to this table yet. See `TODO(contract_label_review)` comment in `oli_airtable.py` line ~564.  
> **TODO:** `airtable_read_label_pool_reattest()` does not write to this table yet. See `TODO(contract_label_review)` comment in `oli_airtable.py` line ~504.  
> **TODO:** Airtable "Label Pool Automated" needs two new fields: `gtp_owner_project` (text) + `gtp_no_owner_project` (checkbox).  
> **TODO:** Airtable "Label Pool Reattest" needs new field: `gtp_no_owner_project` (checkbox).

| Column | Type | Description |
|---|---|---|
| `id` | `serial` | PK (append-only log) |
| `address` | `bytea` | Contract address (FK → contract_label_features) |
| `origin_key` | `varchar` | Chain (FK → contract_label_features) |
| `labeled_at` | `timestamptz` | When the AI ran — from `contract_label_features.labeled_at` |
| `reviewed_at` | `timestamptz` | When the GTP attester re-attested |
| `review_source` | `varchar` | `airtable_automated` / `airtable_reattest` / `airtable_unlabelled` |
| **AI values (frozen at labeling time)** | | |
| `ai_contract_name` | `varchar` | What the AI assigned — from `contract_label_features` |
| `ai_usage_category` | `varchar` | What the AI assigned — from `contract_label_features` |
| `ai_confidence` | `numeric` | AI confidence at labeling time |
| `ai_owner_project_likely` | `bool` | Was `protocol-contract-likely` sentinel fired |
| **GTP-attested values (what was ultimately signed by `OLI_gtp_pk`)** | | |
| `gtp_contract_name` | `varchar` | Human-corrected name; NULL = kept AI name unchanged |
| `gtp_usage_category` | `varchar` | Human-corrected category; NULL = kept AI category unchanged |
| `gtp_owner_project` | `varchar` | Protocol slug assigned by human (e.g. `uniswap`, `aave`); NULL = no owner assigned |
| `gtp_no_owner_project` | `bool` | TRUE = human explicitly confirmed this contract has no protocol owner — prevents re-surfacing for owner assignment |
| **Derived diff flags (generated columns)** | | |
| `name_was_changed` | `bool` | `gtp_contract_name IS NOT NULL` |
| `category_was_changed` | `bool` | `gtp_usage_category IS NOT NULL` |
| `owner_was_assigned` | `bool` | `gtp_owner_project IS NOT NULL` |

---

## DDL

```sql
-- ── Feature store ────────────────────────────────────────────────────────────
CREATE TABLE public.contract_label_features (
    -- Identity
    address               bytea        NOT NULL,
    origin_key            varchar      NOT NULL,
    labeled_at            timestamptz  NOT NULL,
    blockscout_name       varchar,
    is_verified           bool         DEFAULT false,
    is_proxy              bool         DEFAULT false,
    impl_address          bytea,
    impl_name             varchar,
    impl_verified         bool         DEFAULT false,
    has_github_repo       bool         DEFAULT false,
    github_repo_count     int          DEFAULT 0,
    -- Activity metrics
    metric_day_range      int,
    txcount               bigint,
    gas_eth               numeric,
    avg_daa               numeric,
    avg_gas_per_tx        numeric,
    rel_cost              numeric,
    success_rate          numeric,
    daa_ratio             numeric,
    -- Caller diversity
    caller_diversity_pct    numeric,
    caller_diversity_label  varchar,
    unique_callers          int,
    total_traces_sampled    int,
    -- Trace-derived protocol signals
    novel_tokens              text[],
    common_tokens             text[],
    bs_token_transfers        jsonb,
    matched_routers           text[],
    matched_dex_pools         text[],
    calls_into_dex_pool_swap  bool     DEFAULT false,
    matched_lending           text[],
    matched_staking           text[],
    matched_bridges           text[],
    matched_oracle_fns        text[],
    delegates_to              varchar,
    raw_delegate              bool     DEFAULT false,
    all_traces_empty          bool     DEFAULT false,
    traces_only_raw_opcodes   bool     DEFAULT false,
    named_contracts_in_traces text[],
    -- Log signals
    log_has_access_control   bool DEFAULT false,
    log_has_token_transfers  bool DEFAULT false,
    log_has_swaps            bool DEFAULT false,
    log_has_bridge_events    bool DEFAULT false,
    log_has_erc4337          bool DEFAULT false,
    log_has_staking          bool DEFAULT false,
    log_has_governance       bool DEFAULT false,
    log_category_hints       text[],
    log_top_events           jsonb,
    -- AI output
    ai_contract_name        varchar,
    ai_usage_category       varchar,
    ai_confidence           numeric,
    ai_reasoning            text,
    ai_model                varchar,
    ai_owner_project_likely bool    DEFAULT false,
    sentinel_fired          bool    DEFAULT false,
    sentinel_category       varchar,
    PRIMARY KEY (address, origin_key)
);

CREATE INDEX ON public.contract_label_features (origin_key, labeled_at DESC);
CREATE INDEX ON public.contract_label_features (ai_confidence) WHERE ai_confidence < 0.7;
CREATE INDEX ON public.contract_label_features (ai_usage_category);


-- ── Review audit log ─────────────────────────────────────────────────────────
CREATE TABLE public.contract_label_review (
    id                    serial       PRIMARY KEY,
    address               bytea        NOT NULL,
    origin_key            varchar      NOT NULL,
    labeled_at            timestamptz,
    reviewed_at           timestamptz  NOT NULL,
    review_source         varchar      NOT NULL,
    -- AI values frozen at labeling time
    ai_contract_name        varchar,
    ai_usage_category       varchar,
    ai_confidence           numeric,
    ai_owner_project_likely bool,
    -- GTP-attested values
    gtp_contract_name    varchar,
    gtp_usage_category   varchar,
    gtp_owner_project    varchar,
    gtp_no_owner_project bool         DEFAULT false,
    -- Diff flags
    name_was_changed     bool GENERATED ALWAYS AS
                           (gtp_contract_name IS NOT NULL) STORED,
    category_was_changed bool GENERATED ALWAYS AS
                           (gtp_usage_category IS NOT NULL) STORED,
    owner_was_assigned   bool GENERATED ALWAYS AS
                           (gtp_owner_project IS NOT NULL) STORED
);

CREATE INDEX ON public.contract_label_review (address, origin_key);
CREATE INDEX ON public.contract_label_review (reviewed_at DESC);
CREATE INDEX ON public.contract_label_review (name_was_changed)     WHERE name_was_changed     = true;
CREATE INDEX ON public.contract_label_review (category_was_changed) WHERE category_was_changed = true;
CREATE INDEX ON public.contract_label_review (owner_was_assigned)   WHERE owner_was_assigned   = true;
```

---

## Notes for ML Use

### Feature encoding

- **`text[]` columns** — one-hot at query time with `unnest()`, or embed as bag-of-words. Do not flatten in DB.
- **`jsonb` columns** (`bs_token_transfers`, `log_top_events`) — extract structured features at training time. Token names are high-cardinality free text; embed separately.
- **`ai_confidence`** — use as sample weight, not a filter. Low-confidence rows are the hardest and most informative training cases.
- **`daa_ratio`** — derived feature already computed (`avg_daa / txcount × 100`); captures automation signal without requiring both columns.

### Label quality tiers

| Tier | Filter | Use for |
|---|---|---|
| **High quality** | `r.gtp_usage_category IS NOT NULL OR r.gtp_owner_project IS NOT NULL` | Validation set, fine-tuning |
| **Medium quality** | `f.ai_confidence >= 0.85 AND r.id IS NULL` | Pre-training with weak labels |
| **Error analysis** | `r.category_was_changed = true` | Where AI got it wrong |
| **Hard cases** | `f.ai_confidence < 0.7` | Active learning, boundary cases |
| **Owner ground truth** | `r.owner_was_assigned = true` | Protocol identity supervision |
| **Explicit no-owner** | `r.gtp_no_owner_project = true` | Negative examples for owner classifier |

### Model ideas

| Task | Features | Target |
|---|---|---|
| Category classifier | All `contract_label_features` signal columns | `ai_usage_category` (weak) or `gtp_usage_category` (strong) |
| Name correction predictor | Feature signals | `name_was_changed` — predict when AI name will be wrong |
| Category correction predictor | Feature signals | `category_was_changed` — predict AI category errors |
| Owner project classifier | `novel_tokens`, `matched_*`, `log_*`, `ai_usage_category` | `gtp_owner_project` |
| Sentinel bypass | Cheap signals only (metrics + log flags, no traces) | Category — skip Gemini for obvious cases |
| Confidence calibration | Features + `ai_confidence` | `category_was_changed` — is AI confidence predictive of errors? |

### What is NOT stored here

- Raw Tenderly traces — too large; computed transiently during enrichment
- Raw Blockscout API responses — derive features from them instead  
- ABI or source code — too large; `is_verified` + `blockscout_name` are the proxies
- Revocation events — tracked separately in `public.labels` (OLI standard)

---

## Implementation Status

| Component | Status |
|---|---|
| `ai_classifier.py` — extend return dict with pre-computed signals | **TODO** — see `TODO(contract_label_features)` comment at line ~994 |
| `automated_labeler.py` — write to `contract_label_features` after classification | **TODO** — see `TODO(contract_label_features)` comment at line ~895 |
| `db_connector.py` — `upsert_contract_label_features()` | **TODO** — see `TODO(contract_label_features)` comment after `get_oli_labels()` |
| `db_connector.py` — `insert_contract_label_review()` + `get_contract_label_features()` | **TODO** — same comment block |
| `oli_airtable.py` `airtable_read_automated_labels()` — diff capture | **TODO** — see `TODO(contract_label_review)` comment at line ~564 |
| `oli_airtable.py` `airtable_read_label_pool_reattest()` — diff capture | **TODO** — see `TODO(contract_label_review)` comment at line ~504 |
| Airtable "Label Pool Automated" — add `gtp_owner_project` + `gtp_no_owner_project` fields | **TODO** — manual Airtable UI change |
| Airtable "Label Pool Reattest" — add `gtp_no_owner_project` field | **TODO** — manual Airtable UI change |
| Run table DDLs on DB | **TODO** — after code changes are deployed |
