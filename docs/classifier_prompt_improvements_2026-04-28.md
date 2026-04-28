# Classifier Prompt + Logic Improvements — 2026-04-28

Author: rolling notes
Scope: `backend/labeling/ai_classifier.py` (system prompt + response schema), `backend/labeling/automated_labeler.py` (sentinel), `backend/labeling/eval/` (self-critique loop).

## Why

After 109 human-validated rows from the 2026-04-24 labeling round + the 2026-04-27 batch, the classifier showed three persistent failure modes that human review kept catching:

1. The model would correctly reason `→ trading` / `→ oracle` / `→ bridge` but emit `usage_category: "other"`. **80 / 127 rows in the baseline.**
2. Generic `AmbiguousContract` names appeared even when traces had clear protocol identity, because Blockscout token-transfer names dominated naming for simple proxies.
3. Verified or github-linked protocol contracts were misclassified as private trading bots in PATH G1, because the EXCEPTION clause was buried inside the path.

A self-critique eval loop (`backend/labeling/eval/`) was built to drive this iteratively: re-classify rows that have human corrections, ask `gemini-2.5-pro` to diagnose what went wrong against the current system prompt, and aggregate the proposed prompt edits across runs. Three iteration rounds (v1 baseline → v2 → v3) followed.

## Quantified outcome

| metric | v1 baseline | v2 (schema) | v3 (prompt) | Δ vs v1 |
|---|---|---|---|---|
| Rows with errors (of 127) | 106 | 108 | 102 | -4 |
| Reasoning↔output contradictions | **80** | 0 | 0 | **-80** |
| `usage_category="other"` (hedging) | 127 | 5 | 7 | -120 |
| **High-severity errors** | **79** | 41 | **26** | **-67%** |
| Medium severity | 27 | 52 | 56 | +29 |
| Low severity | 21 | 34 | 45 | +24 |
| Meta-eval failures | 20 | 6 | 10 | -10 |

Severity distribution shifted decisively toward less-severe errors: high cut by two-thirds, with the residual moving into medium and low buckets where the model is wrong but not catastrophically.

## Round-by-round changes

### Round 1 (v2) — Response schema fix (the single biggest win)

**Root cause.** The Gemini structured-output schema declared `properties` in the order `contract_name`, `usage_category`, `confidence`, `reasoning` with `required=["contract_name", "usage_category"]`. With no `property_ordering`, Flash generated `usage_category` BEFORE `reasoning` — committing to a category before deriving the path conclusion. The safe hedge was `"other"`. The reasoning text, generated afterward, would then conclude `→ trading` (etc.), producing the 80/127 contradictions.

Additionally, `usage_category` had no `enum` constraint. The post-process at the bottom of `classify_contract()` rewrites unknown categories to `fallback_id` (`"other"`), compounding the problem when Flash hallucinated category strings.

**Fix.** `backend/labeling/ai_classifier.py:925-955`:
- `enum=valid_ids` on `usage_category` (constrains to OLI valid IDs, eliminates fallback rewrites).
- `property_ordering=["reasoning", "contract_name", "usage_category", "confidence"]` (forces reasoning first).
- `required=["reasoning", "contract_name", "usage_category"]` (reasoning is no longer optional).

**Result.** 80 contradictions → 0. `"other"` hedging dropped from 127 / 127 to 5 / 127. High-severity errors dropped from 79 to 41.

### Round 1 (v2) — First batch of prompt edits

Driven by the v1 eval's ranked proposals (PATH A had 17 distinct proposals at avg conf 0.91):

- **HOW TO CLASSIFY**: added GitHub repository as a third primary source of truth alongside verified name and trace behavior; added an "Output discipline" rule that `reasoning` must end with `→ <category>` matching `usage_category`.
- **PATH A — protocol-specific corrections**: added `Socket* → bridge`, `*OffchainAggregator / AccessControlledOffchainAggregator → oracle` (Chainlink), `OpenOceanSwapModule → nft_marketplace`, `ConditionalTokens / CTFExchange / NegRiskAdapter → prediction_markets` (Polymarket pattern), `IdRegistry / *Registrar → identity` (Farcaster, ENS).
- **PATH O — Oracle Infrastructure**: added `transmit`, `fulfillOracleRequest`, `fulfillOracleRequest2`, `postPrices` to the oracle function list (Chainlink Operator pattern). Made oracle classification "sticky" — once determined, do not fall back. Added a soft-exception so a single incidental getter call doesn't outweigh dominant non-oracle activity.
- **PATH G3 — Public Callers**: added the low-success-rate override (`success_rate < 60%` + DEX traces → `trading` MEVBot/ArbitrageBot, even with PUBLIC callers — many callers competing and failing is the MEV signature).
- **name-derivation**: explicit priority order (verified name > traces > novel_tokens > behavioral). Forbid naming a contract after a `bs_token_transfers` token when `named_contracts_in_traces` is non-empty.

### Round 2 (v3) — Round-2 prompt edits + the avg_daa discriminator

The v2 report surfaced new top patterns once the contradiction class was eliminated. Key additions:

- **PATH O**: added `updatePrices` (Stork-pattern price pushers were being missed).
- **PATH A**: added `*Factory / *Deployer / *Creator → developer_tools` (factories should be classified by their role as deployment infra, not by what they deploy). Added `TransparentUpgradeableProxy / ERC1967Proxy → developer_tools` for cases where the generic proxy name does reach the classifier.
- **PATH G1 EXCEPTION**: extended `verified contracts` to `verified contracts OR github.has_valid_repo=true`. Added an exception-to-the-exception: verified or github-linked contracts whose dominant activity is automated LP management (CLPool/CLGauge/NonfungiblePositionManager mint/burn/collect) ARE `trading`, even with confirmed identity. Symmetric edit applied to PATH G2.
- **PATH G3**: stablecoin name-pattern as primary signal (`USD/Stable/USDC/USDT/USDM/BUSD/TUSD/FRAX/LUSD` → `stablecoin`); airdrop pattern recognition (`Claim/Airdrop/Distributor/Vesting/Rewards/Points` with broad user base → `airdrop`); explicit avg_daa gate referenced.

#### avg_daa Discriminator (new dedicated section)

Reviewer feedback: the strongest discriminator between a private trading bot and public DEX infrastructure is **absolute avg_daa relative to chain_median_daa**, but the prompt was treating `avg_daa` as a tiebreaker. Added a dedicated section with three explicit tiers:

  - **HIGH** (`avg_daa ≥ chain_median_daa AND avg_daa ≥ 50`): public infrastructure by definition — many independent users. Cannot be a private trading bot regardless of how trading-shaped the traces look. Even bot-shaped DEX activity is overridden to `dex` / `bridge` / `oracle` / etc. by trace content.
  - **LOW** (`avg_daa < max(chain_median_daa × 0.05, 3)`): private operator(s). The `DEXAggregator` label is invalid at this DAA level unless callers can be confirmed as real public EOAs (not admin/keeper EOAs cycling addresses). LOW + `success_rate < 90%` + DEX activity → `trading` (MEVBot/ArbitrageBot). LOW + `success_rate ≥ 90%` + DEX activity → `trading` (StrategyExecutor / [Protocol]Rebalancer).
  - **MID**: examine caller diversity and success rate; default to `trading` when in doubt. `DEXAggregator` is a strong claim that requires strong evidence.

Hard rule: when picking the name `DEXAggregator`, the model MUST cite `avg_daa ≥ chain_median_daa` in the reasoning. Otherwise use `StrategyExecutor` or `MEVBot`.

The same primacy was reinforced in `HOW TO CLASSIFY` so the model sees `avg_daa` as a primary public/private signal before reading any path.

### Round 3 (v3) — PATH 0 Identity Gate

The v3 eval revealed that the model was still anchoring on PRIVATE caller diversity in PATH G1 and skipping the EXCEPTION clause that mentioned `github.has_valid_repo`. Examples:
- `0x00003bf45c…` (Curve PMM, github=true) → still classified `trading / StrategyExecutor`.
- `0xee7ae85f2…` (verified single-owner USDC vault) → classified `stablecoin` instead of `trading`.

Buried EXCEPTIONS aren't enough — the check has to run before any caller-based path.

**Fix.** `backend/labeling/ai_classifier.py` now defines `PATH 0 — Identity Gate` that runs BEFORE PATH A. If `is_verified=True OR github.has_valid_repo=true`, PATH G1/G2/G3 trading defaults are explicitly disabled. The single exception (verified LP managers) is preserved. The repo-name owner-extraction heuristic is also stated here (e.g. `TransitSwapRouterV5` → transit-finance).

## Code-level fix — Blockscout token gate

`backend/labeling/ai_classifier.py:907-927` (the `novel_tokens` augmentation block).

Previously every token in `bs_token_transfers` was unconditionally appended to `novel_tokens`. Simple proxies / forwarders that pass through random ERC20 transfers ended up named after the largest passing-through token, swamping trace-derived identity (P2 in the original validation findings).

After the change, a Blockscout token is promoted to `novel_tokens` only if its name or symbol also appears in the trace-decoded contract names, OR the trace set is entirely empty (no competing identity). Full `bs_token_display` still flows into the prompt as context — this is gating the augmentation only, not removing visibility.

## Sentinel (`_is_protocol_likely`) — already correct

`backend/labeling/automated_labeler.py:538`.

Reviewed during this work. Already covers all three positive overrides identified in the eval:
- `is_verified=true` → `True` (line 549).
- `novel_tokens` non-empty → `True` (line 562).
- `github.has_valid_repo=true` → `True` (line 589, via the `log_signals['has_valid_repo']` plumbing at line 637).

No change needed.

## Files touched

| File | Change |
|---|---|
| `backend/labeling/ai_classifier.py` | Schema fix (enum + property_ordering); new PATH 0 Identity Gate; PATH A protocol corrections (Socket/OffchainAggregator/Factory/etc.); PATH O updatePrices/fulfillment fns + sticky rule; PATH G1/G2 EXCEPTIONs extended to github + LP exception-to-exception; PATH G3 stablecoin/airdrop/avg_daa rules; new avg_daa Discriminator section; name-derivation priority + composite-forbid; BS token augment gate. |
| `backend/labeling/automated_labeler.py` | No changes (sentinel already correct). |
| `backend/labeling/eval/` | New package: `context_cache.py`, `eval_human_corrections.py`, `re_eval_prompt.py`, `README.md` — the self-critique loop driving these iterations. |
| `docs/classifier_prompt_improvements_2026-04-28.md` | This report. |

## How to re-run the eval

```bash
# Cache is already warm (127 rows enriched on 2026-04-27).
python backend/labeling/eval/eval_human_corrections.py --run-id v4_<date>

# Reports land at backend/local/eval_results/<run_id>/{per_row.jsonl, report.md}.
```

The `per_row.jsonl` is shaped as `{state, action, reward}` analogues for downstream RL — once `contract_label_features` + `contract_label_review` are populated, the dump-loader inside the orchestrator can swap to a SQL query without other structural changes.

## Known residual issues (v3)

- Stablecoin rule still occasionally over-fires on private contracts that move USDC as part of a single-owner vault (PATH 0 Identity Gate should fix most of these on the next eval).
- Owner extraction from `github.has_valid_repo` URL is named in PATH 0 but not yet wired with strong heuristics for repo-name → owner-slug mapping. Best handled when the linked repository URL becomes part of the classifier input rather than a boolean.
- ~10 / 127 rows still produce meta-eval JSON parse failures from `gemini-2.5-pro` with very long oracle/bridge contracts. Acceptable noise floor; the per-row JSONL records `_meta_error` so they can be re-evaluated.

## What we did NOT change

- Sentinel `_is_protocol_likely` — already covers `is_verified`, `novel_tokens`, `has_valid_repo`. No edits made.
- The 22-key signal extension to `classify_contract()`'s return dict (TODO at `ai_classifier.py:~1095`) — not required for prompt iteration, but it would let the meta-evaluator see exact path-side signals (current state shows the model only some of them via `RAW_SIGNALS` derived from cached context). Worth doing before the next eval round.
- The downstream `contract_label_features` / `contract_label_review` write paths — separate workstream, tracked in the prior plan.
