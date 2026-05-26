---
name: airtable-labeling-patterns
description: Use when working with Airtable in the labeling pipeline — linked-record resolution, human override coalesce, dedup before push, sentinel slug handling, diff capture, or questions about which table/reader/column to use.
---

# Airtable Labeling Patterns

## Two tables, two readers — never mix them

| Table | Reader | Flow |
|-------|--------|------|
| `Label Pool Automated` | `read_all_label_pool_automated` | GTP attester — human override coalesce happens here |
| `Label Pool Reattest` | `read_all_label_pool_reattest` | External attester — no coalesce, read as-is |

Both readers live in `backend/src/misc/airtable_functions.py`. Never point them at each other's table.

## Human override coalesce (`read_all_label_pool_automated`)

Applied automatically before the call site sees the data:

```
contract_name  ← contract_name_human  if human set it, else AI value
usage_category ← new_usage_category   if human set it, else AI value
```

**AI baseline is snapshotted BEFORE coalesce** into `ai_contract_name` / `ai_usage_category` columns so diff capture downstream sees both sides.

**Never edit the AI columns (`contract_name`, `usage_category`) in place** — always use `contract_name_human` / `new_usage_category` for human corrections.

## Diff capture → `contract_label_review`

Happens in `oli_airtable.py:airtable_automated_attest` at attest time:

```python
gtp_name = tags['contract_name']  if tags['contract_name'] != ai_name else None
gtp_cat  = tags['usage_category'] if tags['usage_category'] != ai_cat  else None
# Only set when human actually changed the AI value
db_connector.insert_contract_label_review([{...}])
```

`ai_name`/`ai_cat` come from `contract_label_features` first, falling back to Airtable `ai_contract_name`/`ai_usage_category` if features write failed. The features-table path is stronger — it has the original classify_contract() output.

## Linked-record fields

Linked-record fields arrive as 1-element lists of `recXXX` IDs, not slugs:
```python
# Wrong — will write a recID as the slug
owner_project = row['owner_project']

# Right — resolve first
owner_project = resolve_recid_to_slug(row['owner_project'][0])
```

When writing linked records BACK to Airtable, wrap the slug/recID in a list:
```python
df['owner_project'] = df['owner_project'].apply(lambda x: [x] if pd.notna(x) else None)
```

Resolution lives in `airtable_functions.py`. Chain recIDs use the `Chains` table `caip2` column. Project recIDs use the project name → recID mapping.

## Dedup before push

Always dedup against existing Airtable rows before `push_to_airtable()`. Duplicate rows accumulate silently — Airtable has no unique-key enforcement.

Pattern used in `oli_airtable.py`:
```python
df_new = df_attested.merge(df_in_airtable, on=[...keys...], how='left', indicator=True)
df_new = df_new[df_new['_merge'] == 'left_only'].drop(columns=['_merge'])
```

## Sentinel slug: `protocol-contract-likely`

Set by `_is_protocol_likely()` in `automated_labeler.py` as `owner_project` placeholder when a contract looks like a public protocol but has no confirmed owner yet.

**Rules:**
- Surfaces the contract in `Label Pool Automated` for human `owner_project` / `temp_owner_project` / `gtp_no_owner_project` assignment.
- **Must be dropped before OLI attestation** — it is not a valid OLI owner slug.
- `read_all_label_pool_automated` drops it automatically: `df.loc[df['owner_project'] == 'protocol-contract-likely', 'owner_project'] = None`
- `gtp_no_owner_project=True` also drops `owner_project` from the attestation payload.

## `temp_owner_project`

Free-text field, review-only. Stored in `contract_label_review`, never attested on-chain. Used by reviewers to note a suspected owner before an official OLI slug exists.

## Push helper

`push_to_airtable(table, df)` in `airtable_functions.py` — keeps list-like fields (linked-record arrays) untouched. Use this, not raw pyairtable batch calls, to avoid accidentally flattening linked records.
