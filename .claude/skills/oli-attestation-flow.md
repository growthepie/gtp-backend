---
name: oli-attestation-flow
description: Use when working on OLI attestation from Airtable — what gets submitted, owner_project handling, the reattest loop, or DAG task wiring for oli_airtable.py.
---

# OLI Attestation Flow

## Reattest path

```
External attester  →  'Label Pool Reattest'  →  airtable_read_label_pool_reattest  →  OLI
```

- Rows are written to 'Label Pool Reattest' by the hourly `oli_reattest` DAG (not by `oli_airtable.py` — its 24h lookback would recreate rows reviewers deleted).
- Reader: `read_all_label_pool_reattest` in `airtable_functions.py`. Do not change this function.
- Human approves a row → `oli.submit_label()` with the gtp key → row deleted from Airtable. Failed rows stay approved and are retried on the next run.

The former 'Label Pool Automated' flow (AI labeler, protocol-likely surfacing, `contract_label_review` diff log) has been removed.

## OLI tags payload

```python
tags = {
    'contract_name': ...,
    'usage_category': ...,
    'owner_project': ...,
}
```

`chain_id` comes from `main_config.py` (caip2) — never hardcode.

## DAG schedule (`oli_airtable.py`)

```
schedule='50 00 * * *'  # 0:50am
# After: coingecko, oli_oss_directory, oli_misc_sync
# Before: metrics_sql_blockspace
```

Human approval is the only gate — there is no confidence threshold.

## Common failure modes

| Symptom | Cause | Fix |
|---------|-------|-----|
| Duplicate attestations on OLI | Row not deleted from Airtable after first attest | Verify the delete step ran; check for Airtable API errors |
| `owner_project` recID in OLI payload | Linked-record not resolved before submit | Resolve recIDs to slugs before building tags |
| Row stuck in Airtable, never attested | `approve` column not set to True | Human must check 'approve' checkbox in Airtable |
