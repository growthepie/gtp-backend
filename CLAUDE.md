# gtp-backend — Claude Code conventions

## Skills index

Detailed reference guides live in `.claude/skills/`. Load them when debugging the relevant subsystem.

| Skill | When to use |
|-------|-------------|
| `airtable-labeling-patterns.md` | Linked-record resolution, human override coalesce, dedup, sentinel handling |
| `oli-attestation-flow.md` | OLI submission rules, owner_project handling, reattest loop |
| `rpc-discovery.md` | Finding new public RPCs (chainlist + chain docs), auditing `sys_rpc_config`, raw DAGs failing for lack of RPCs |

---

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

- **Never hardcode** `origin_key → chain_id`, `origin_key → caip2`, or chain names in DAGs.
- Import from `main_config` or use `DbConnector` helpers (`get_chain_info`, `get_stages_dict`).
- Airtable linked-record resolution uses the `Chains` table (`caip2` column → recID). Do not hardcode caip2 strings.

## db_connector.py — additive only

`backend/src/db_connector.py` is a shared dependency used across all DAGs and adapters.

- **Only add new methods. Never edit existing ones** unless fixing a confirmed bug — changing a method signature or query silently breaks every caller.
- New methods go at the bottom of the `DbConnector` class.
- Name pattern: `get_*` for reads, `upsert_*` for upsert, `insert_*` for append-only writes, `execute_*` for raw queries.
- Upserts use `upsert_table(table_name, df)` via pangres — set `(address, origin_key)` or the natural PK as the DataFrame index before calling.
- Append-only tables (audit logs) use raw SQL INSERT, never `upsert_table`.
- `upsert_table` called with `public.<table>` prefix → pangres double-qualifies to `public."public.<table>"` → `UndefinedTable`. Pass bare table name only.

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

## Airtable patterns

- Linked-record fields return `recXXX` IDs — always resolve to slugs before use (see `airtable_functions.py`).
- When writing linked records back to Airtable, wrap in a list: `[recXXX]`.
- Dedup against existing Airtable rows before any `push_to_airtable` call to avoid duplicates.
- `temp_owner_project` (Unlabelled table) — free text, review-only; `clear_all_airtable` keeps rows that have it. Never attested on-chain.
