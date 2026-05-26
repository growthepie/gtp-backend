---
name: automated-labeler-pipeline
description: Use when debugging why contracts are missing, skipped, not enriched, not classified, not written to Airtable, or not attested — covers the full fetch→enrich→classify→features→airtable→attest stage map and its common failure modes.
---

# Automated Labeler Pipeline

## Stage order

Single `label_and_attest` Airflow task in `oli_automated_labeler.py`. Each stage can fail independently.

```
1. FETCH      automated_labeler.py:_fetch_unlabeled_contracts_v2()
              SQL: blockspace_fact_contract_level → dedup against existing Airtable rows

2. ENRICH     concurrent_contract_analyzer.py
              Blockscout (name, verified, proxy) + GitHub + Tenderly traces + on-chain metrics
              ⚠ contracts with EMPTY enrichment are SKIPPED at automated_labeler.py:619

3. CLASSIFY   ai_classifier.py:classify_contract()
              Builds prompt with pre-computed signals → Gemini Flash → {contract_name,
              usage_category, confidence, reasoning, caller_diversity_label, novel_tokens, …}

4. FEATURES   db_connector.upsert_contract_label_features() → public.contract_label_features
              Idempotent on (address, origin_key). NON-FATAL — pipeline continues if this fails.

5. AIRTABLE   _write_attested_to_airtable() → 'Label Pool Automated'
              ALL rows land here for human review, regardless of confidence.

6. ATTEST     oli.submit_label_bulk()
              Only rows with confidence >= confidence_threshold (default 0.3) are submitted to OLI.
```

## Key files

| File | Role |
|------|------|
| `backend/labeling/automated_labeler.py` | Orchestrator, enrichment skip logic, `_build_feature_row`, Airtable write |
| `backend/labeling/ai_classifier.py` | Gemini classifier, signal pre-computation, prompt construction |
| `backend/labeling/concurrent_contract_analyzer.py` | Parallel enrichment (Blockscout, GitHub, Tenderly) |
| `backend/airflow/dags/oli/oli_automated_labeler.py` | Airflow DAG wrapper |

## Enrichment skip logic (`automated_labeler.py:619`)

A contract is skipped (never classified) when ALL of these are empty:
- `blockscout.contract_name`
- `blockscout.is_verified`
- `traces`
- `address_logs`

These contracts would fall to PATH G0 → `other/AmbiguousContract 0.30` due to empty API returns — not genuine ambiguity. Skipping prevents polluting the attestation pool.

**Symptom:** Contract never appears in Airtable or `contract_label_features`.
**Debug:** Check Blockscout API key, Tenderly key, network connectivity to the chain's explorer.

## `_is_protocol_likely()` (`automated_labeler.py:551`)

Post-classification function that sets `owner_project = 'protocol-contract-likely'` when True.

Decision order (first match wins):
1. `blockscout.is_verified` → **True** (overrides everything — MEV bots rarely verify source)
2. `category in ('trading', 'other')` → **False**
3. `confidence < 0.50` → **False**
4. `success_rate < 0.40` → **False**
5. `novel_tokens` → **True** (gated after trading/other exclusion)
6. `category in ('bridge','lending','erc4337','cc_communication','derivative','stablecoin','oracle')` → **True**
7. Governance/staking/bridge/erc4337 log signals → **True**
8. `confidence >= 0.65` → **True**, else **False**

**`dex` is intentionally excluded from step 6** — unverified dex-classified contracts can be private strategies with public callers.

## `_build_feature_row()` (`automated_labeler.py:680`)

Converts the full classify_contract() return dict into the DB row shape for `contract_label_features`. 
**Never remove keys from `classify_contract()` return dict** without updating this function — the DB upsert will silently drop the column data.

## Chain median DAA

`fetch_chain_median_daa()` uses module-level `_chain_median_cache`. One DB connection per chain per process. Do not call `DbConnector()` inside per-contract loops.

## Common failure modes

| Symptom | Cause | Fix |
|---------|-------|-----|
| Contract missing from Airtable AND features | Enrichment returned empty → skipped at step 2 | Check API keys / Blockscout availability for that chain |
| Contract in Airtable but missing from features | Step 4 (features write) failed non-fatally | Check logs for `upsert_contract_label_features` error; re-run backfill |
| Contract in features but not attested on OLI | `confidence < 0.3` threshold | Lower `--confidence-threshold` or accept — low confidence is intentional |
| `UndefinedTable` on features write | `public.<table>` prefix passed to `upsert_table` | Pass bare table name only — pangres double-qualifies otherwise |
| `protocol-contract-likely` in OLI attestation | Sentinel not stripped before submit | `gtp_no_owner_project=True` or resolve to real slug before attesting |
| "Database connection successful." flooding logs | `DbConnector()` called per contract | Bug — ensure `_chain_median_cache` is in scope, module not reloaded |
