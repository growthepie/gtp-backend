# Classifier Self-Critique Eval

Re-runs `ai_classifier.classify_contract()` on rows humans corrected, then asks
`gemini-2.5-pro` (the meta-evaluator) to diagnose what went wrong and propose
concrete edits to `_SYSTEM_INSTRUCTION`.

## Files

- `context_cache.py` — caches on-chain context (Blockscout/Tenderly/GitHub) per `(address, origin_key)` so prompt iterations don't re-spend API credits. Cache lives at `backend/local/eval_cache/`.
- `re_eval_prompt.py` — meta-evaluator system prompt + JSON response schema.
- `eval_human_corrections.py` — orchestrator. Loads dump, filters to rows with human corrections, reclassifies, meta-evals, persists JSONL + markdown report.

## Quickstart

Smoke test (no meta calls, 3 rows):

```bash
python backend/labeling/eval/eval_human_corrections.py \
    --run-id smoke --limit 3 --skip-meta
```

Full run on every row with a human correction:

```bash
python backend/labeling/eval/eval_human_corrections.py \
    --run-id 2026-04-27_pass1
```

Force-refresh on-chain context (skip cache):

```bash
python backend/labeling/eval/eval_human_corrections.py \
    --run-id 2026-04-27_refresh --refresh
```

Bulk-warm cache only:

```bash
python backend/labeling/eval/context_cache.py --limit 0
```

## Outputs

- `backend/local/eval_results/<run_id>/per_row.jsonl` — one JSON object per evaluated row containing `{address, origin_key, ground_truth, ai_original, ai_rerun, raw_signals, meta_eval, ts}`.
- `backend/local/eval_results/<run_id>/report.md` — aggregated patterns + ranked prompt-edit proposals + per-row diff table.

## Ground-truth filter

A row is evaluated only when at least one of these is set:

- `human_contract_name`
- `human_usage_category` (slug)
- `gtp_owner_project_confirmed=True` AND `owner_project_linked` non-empty
- `gtp_no_owner_project=True`
- `temp_owner_project` non-empty
- `human_comment` non-empty

## Future RL hook

Per-row JSONL is shaped close to (state, action, reward) for offline RL:

- **state** = `ground_truth` + `raw_signals`
- **action** = `ai_rerun`
- **reward signal** = `meta_eval.severity` + per-field correctness comparison

Once `contract_label_review` is populated, swap the dump loader for a SQL
query against `contract_label_review JOIN contract_label_features` — no other
structural changes needed.

## Env required

- `GEMINI_API_KEY` — used for both classifier (Flash) and meta-evaluator (Pro)
- `BLOCKSCOUT_API_KEYS`, `TENDERLY_ACCESS_KEY`, GitHub token — only needed on cache miss
- DB env vars (`DB_*`) — needed only on cache miss (for chain median DAA + metrics)
