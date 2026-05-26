---
name: oli-attestation-flow
description: Use when working on OLI attestation — what gets submitted, confidence thresholds, owner_project handling, protocol-contract-likely sentinel, gtp_no_owner_project flag, the reattest loop, or DAG task wiring for oli_airtable.py.
---

# OLI Attestation Flow

## Two attestation paths

```
Automated labeler  →  'Label Pool Automated'  →  airtable_automated_attest  →  OLI
External attester  →  'Label Pool Reattest'   →  airtable_reattest_attest   →  OLI
```

These are separate DAG tasks in `oli_airtable.py`. They read from different tables and must never be pointed at the same table.

## What gets attested (automated path)

```
oli_automated_labeler.py  →  confidence >= 0.3  →  oli.submit_label_bulk()
                                                     (direct, bypasses Airtable review)

oli_airtable.py:airtable_automated_attest  →  human approves row in 'Label Pool Automated'
                                            →  human overrides applied (coalesce)
                                            →  oli.submit_label() per row
                                            →  db_connector.insert_contract_label_review() (diff log)
                                            →  row deleted from Airtable
```

The automated labeler attests low-confidence rows directly; the re-attest flow is for human-reviewed corrections.

## OLI tags payload

```python
tags = {
    'contract_name': ...,
    'usage_category': ...,
    'owner_project': ...,   # omitted if gtp_no_owner_project=True or sentinel
}
```

`chain_id` comes from `ORIGIN_KEY_TO_CHAIN_ID` (derived from `main_config.py` — never hardcode).

## `owner_project` rules

| State | What happens |
|-------|-------------|
| Real OLI slug | Included in tags payload |
| `protocol-contract-likely` | **Dropped** — sentinel, not a valid slug. Row surfaces for human review. |
| `gtp_no_owner_project=True` | `owner_project` key omitted from OLI payload entirely |
| `temp_owner_project` set | Stored in `contract_label_review` only — never attested |

`read_all_label_pool_automated` handles the sentinel drop automatically. Verify it at call sites if bypassing the reader.

## `protocol-contract-likely` sentinel lifecycle

```
_is_protocol_likely() returns True
        ↓
owner_project = 'protocol-contract-likely' written to Airtable 'Label Pool Automated'
        ↓
airtable_write_protocol_likely_to_automated() (oli_airtable.py:329) surfaces it for human review
        ↓
Human sets owner_project / temp_owner_project / gtp_no_owner_project
        ↓
read_all_label_pool_automated() drops sentinel → airtable_automated_attest() attests with real slug
```

Never let the sentinel reach `oli.submit_label()` — the OLI schema will reject it.

## `contract_label_review` — append-only diff log

Written by `airtable_automated_attest` at attest time. Records the delta between AI output and human-approved value:

```python
{
    'address', 'origin_key',
    'ai_contract_name',   # original classifier output
    'ai_usage_category',
    'gtp_contract_name',  # set only if human changed the AI value
    'gtp_usage_category',
    'gtp_owner_project_confirmed', 'gtp_no_owner_project',
    'human_comment', 'temp_owner_project',
}
```

`gtp_*` fields are `None` when the human accepted the AI value unchanged.

**This table is the ground-truth source for the eval loop (Phase 1 critique).**

## DAG schedule (`oli_airtable.py`)

```
schedule='50 00 * * *'  # 0:50am
# After: coingecko, oli_oss_directory, oli_misc_sync
# Before: metrics_sql_blockspace
```

## Confidence threshold

Default `0.3` in `automated_labeler.py`. Configurable via `--confidence-threshold` CLI arg. Rows below threshold are written to Airtable but NOT submitted to OLI by the automated labeler. The human-review path (`oli_airtable.py`) has no confidence gate — human approval is the gate.

## Common failure modes

| Symptom | Cause | Fix |
|---------|-------|-----|
| `protocol-contract-likely` appears in OLI | Sentinel not dropped before submit | Check `read_all_label_pool_automated` is used, not raw Airtable read |
| Duplicate attestations on OLI | Row not deleted from Airtable after first attest | Verify delete step in `airtable_automated_attest` ran; check for Airtable API errors |
| `owner_project` recID in OLI payload | Linked-record not resolved before submit | Run `resolve_recid_to_slug` before building tags |
| `contract_label_review` missing rows | `contract_label_features` write failed → fallback to Airtable AI columns | Weaker diff signal; re-backfill `contract_label_features` to recover |
| Row stuck in Airtable, never attested | `approve` column not set to True | Human must check 'approve' checkbox in Airtable |
