---
name: airtable-labeling-patterns
description: Use when working with Airtable in the labeling pipeline — linked-record resolution, dedup before push, or questions about which table/reader/column to use.
---

# Airtable Labeling Patterns

## Label Pool Reattest reader

`Label Pool Reattest` is read by `read_all_label_pool_reattest` in `backend/src/misc/airtable_functions.py` (external attester flow — read as-is, no coalesce). Do not change this function.

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

## `temp_owner_project` (Unlabelled table)

Free-text field reviewers fill in while a label is unfinished. `clear_all_airtable` keeps rows that have it set, and `airtable_write_contracts` dedups against them. Never attested on-chain.

## Push helper

`push_to_airtable(table, df)` in `airtable_functions.py` — keeps list-like fields (linked-record arrays) untouched. Use this, not raw pyairtable batch calls, to avoid accidentally flattening linked records.
