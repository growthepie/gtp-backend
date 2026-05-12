# gtp-backend — Claude Code conventions

## General rule — check for existing mappings first

Before adding any new constant, dict, lookup table, or mapping anywhere in the codebase:
1. Search `backend/src/main_config.py` — chain/origin_key/caip2/ecosystem mappings live here.
2. Search `backend/src/db_connector.py` — DB-backed lookups (stages, maturity, chain info) live here.
3. Search `backend/src/misc/airtable_functions.py` — Airtable recID ↔ slug resolution lives here.
4. `grep` the codebase for the concept (e.g. `ORIGIN_KEY`, `CHAIN_ID`, `caip2`) before writing a new dict.

If a centralized mapping already exists, import and reuse it. Never maintain a parallel copy — they will diverge. Only create a new mapping if none exists and it genuinely belongs to the module you're working in.

---

## Chain / origin_key mappings

All chain mappings live in `backend/src/main_config.py` (`get_main_config`, `get_main_config_dict`).

- **Never hardcode** `origin_key → chain_id`, `origin_key → caip2`, or chain names in DAGs or labeling code.
- Import from `main_config` or use `DbConnector` helpers (`get_chain_info`, `get_stages_dict`).
- Labeling code uses `ORIGIN_KEY_TO_CHAIN_ID` — this dict must be derived from `main_config`, not maintained separately.
- Airtable linked-record resolution uses the `Chains` table (`caip2` column → recID). Do not hardcode caip2 strings.

## db_connector.py — additive only

`backend/src/db_connector.py` is a shared dependency used across all DAGs and adapters.

- **Only add new methods. Never edit existing ones** unless fixing a confirmed bug — changing a method signature or query silently breaks every caller.
- New methods go at the bottom of the `DbConnector` class.
- Name pattern: `get_*` for reads, `upsert_*` for upsert, `insert_*` for append-only writes, `execute_*` for raw queries.
- Upserts use `upsert_table(table_name, df)` via pangres — set `(address, origin_key)` or the natural PK as the DataFrame index before calling.
- Append-only tables (audit logs) use raw SQL INSERT, never `upsert_table`.

## requirements-new.txt

`backend/requirements-new.txt` is the pinned dependency manifest for the Airflow environment.

- **Add any new package here with pinned version** before merging a branch that imports it.
- Packages silently dropped during upstream merges will break all Gemini / external API calls at runtime with no import error until the DAG runs. Always diff `requirements-new.txt` when rebasing onto main.
- Known risk package: `google-genai` — has been dropped in past merges. Verify it's present after every rebase.

## Airflow DAG conventions

### start_date

- `start_date` must be set to a **past date before the first intended run**, never `datetime.now()` or a future date.
- For new DAGs: set `start_date` to the day before the first expected production run.
- For DAGs with `catchup=False` (default): Airflow will run once on deploy if `start_date` is in the past and no prior run exists. Verify this is safe before merging.
- When a DAG's `start_date` is wrong, fix it as a standalone commit with a `fix(dag):` prefix before the feature lands.

### Task structure

- Each `@task()` function must be self-contained — import all dependencies inside the function body, not at module level (Airflow serializes task definitions).
- Keep concern separation: tasks that read from Airtable must not also write to Airtable in the same function unless atomicity is required.
- Diff capture (AI vs human values) belongs in the read task, before melt/submit — never after.

### DAG wiring

- Use `>>` operator to express task dependencies explicitly. Don't rely on implicit ordering.
- Parallel tasks use `[task_a, task_b]` syntax.
- Document the timing rationale in a comment next to `schedule` — e.g. "after coingecko, before metrics_sql_blockspace".

### Table targeting

- `Label Pool Reattest` — external attester flow. Reader: `read_all_label_pool_reattest`. Do not change this function.
- `Label Pool Automated` — GTP attester flow. Reader: `read_all_label_pool_automated`. Human override coalesce happens here.
- Never point both readers at the same table.

## Airtable patterns

- Linked-record fields return `recXXX` IDs — always resolve to slugs before use (see `airtable_functions.py`).
- When writing linked records back to Airtable, wrap in a list: `[recXXX]`.
- Dedup against existing Airtable rows before any `push_to_airtable` call to avoid duplicates.
- Human correction columns: `contract_name_human`, `new_usage_category` — these override AI values. Never edit the AI column (`contract_name`, `usage_category`) in place.
- `temp_owner_project` — free text, review-only, stored in `contract_label_review`, never attested on-chain.
- `gtp_no_owner_project=True` — drops `owner_project` from OLI attestation.
- `protocol-contract-likely` sentinel slug — must be dropped before attestation, never written to OLI.

## Labeling / classifier

- Classifier: `backend/labeling/ai_classifier.py`, model `gemini-2.5-flash`, temp 0.1.
- Response schema **must** include `property_ordering=["reasoning","contract_name","usage_category","confidence"]`. Without this Flash generates `usage_category` before `reasoning` and hedges to "other".
- All signal keys from `classify_contract()` return dict feed into `contract_label_features` — do not remove keys from the return dict without updating `_build_feature_row()` in `automated_labeler.py`.
- Eval loop: `backend/labeling/eval/eval_human_corrections.py` — run against human-corrected rows before and after prompt changes to measure improvement.

## Automated labeler — pipeline & debugging map

### Stage order (single `label_and_attest` Airflow task in `oli_automated_labeler.py`)

1. **Fetch** — `automated_labeler.py:_fetch_unlabeled_contracts_v2()` — SQL query against `blockspace_fact_contract_level`, dedup against existing Airtable rows.
2. **Enrich** — `concurrent_contract_analyzer.py` — Blockscout (name, verified, proxy), GitHub, Tenderly traces, on-chain metrics. Contracts with empty enrichment are skipped (`automated_labeler.py:620`).
3. **Classify** — `ai_classifier.py:classify_contract()` — builds prompt with pre-computed signals, calls Gemini, returns `{contract_name, usage_category, confidence, reasoning, ...}`.
4. **Write features** — `db_connector.upsert_contract_label_features()` → `public.contract_label_features`. Idempotent on `(address, origin_key)`. **Non-fatal if it fails** — pipeline continues.
5. **Write Airtable** — `_write_attested_to_airtable()` → 'Label Pool Automated'. Rows land here for human review regardless of confidence.
6. **Attest OLI** — only rows above `confidence_threshold` (default 0.3) get submitted via `oli.submit_label_bulk()`.

### Human review → re-attest cycle (separate DAG `oli_airtable.py`)

- Human approves a row in 'Label Pool Automated' → `airtable_automated_attest` task fires.
- Reads `contract_label_features` to get original AI values for diff capture (`gtp_name`/`gtp_cat` only set when human changed the AI value).
- Writes diff to `public.contract_label_review` (append-only audit log) via `db_connector.insert_contract_label_review()`.
- Then re-attests on OLI with final tags (human overrides applied), deletes row from Airtable.

### Key files

| File | Role |
|------|------|
| `backend/labeling/automated_labeler.py` | Orchestrator, feature row builder (`_build_feature_row`), Airtable write |
| `backend/labeling/ai_classifier.py` | Gemini classifier, prompt construction, signal pre-computation |
| `backend/labeling/concurrent_contract_analyzer.py` | Parallel enrichment (Blockscout, GitHub, Tenderly) |
| `backend/airflow/dags/oli/oli_automated_labeler.py` | Airflow DAG wrapper — calls `automated_labeler.py` |
| `backend/airflow/dags/oli/oli_airtable.py` | Human review → re-attest → `contract_label_review` diff write |
| `backend/src/db_connector.py:upsert_contract_label_features` | DB write for AI features (pass bare table name, no `public.` prefix) |
| `backend/src/db_connector.py:insert_contract_label_review` | Append-only human corrections audit log |
| `backend/src/misc/airtable_functions.py:read_all_label_pool_automated` | Reads 'Label Pool Automated' for GTP attester flow |

### Key DB tables

| Table | What's in it | Written by |
|-------|-------------|------------|
| `public.contract_label_features` | AI signals + classification per contract run | `automated_labeler.py` (step 4) |
| `public.contract_label_review` | Human correction diffs (AI value vs GTP value) | `oli_airtable.py` at attest time |

### Common failure modes

- `contract_label_features` write fails → Airtable still gets rows; diff capture in `oli_airtable.py` falls back to Airtable `ai_contract_name`/`ai_usage_category` fields (weaker signal).
- `upsert_table` called with `public.<table>` prefix → pangres double-qualifies to `public."public.<table>"` → `UndefinedTable`. Pass bare table name only.
- Enrichment returns no data for a contract → skipped entirely, never reaches classifier (`automated_labeler.py:620`).
- `protocol-contract-likely` sentinel in `owner_project` → must be dropped before OLI attestation; `gtp_no_owner_project=True` drops the field entirely.
