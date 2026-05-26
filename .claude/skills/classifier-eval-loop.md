---
name: classifier-eval-loop
description: Use when the user asks about running, interpreting, or debugging the 3-phase classifier eval pipeline — critique, synthesize, verify, deploy. Triggers on questions about accuracy reports, per_row.jsonl, candidate prompts, phase failures, eval commands, or prompt versioning.
---

# Classifier Eval Loop

## Overview

RL-inspired pipeline that improves `_SYSTEM_INSTRUCTION` in `ai_classifier.py` by critiquing the Gemini classifier against human corrections from `contract_label_review`.

**All eval code lives in:** `backend/labeling/eval/`
**Prompt versions live in:** `backend/labeling/prompts/` (`versions.json`, `current.md`, `v1.md`, …)
**Run outputs live in:** `backend/local/eval_results/<run-id>/`

---

## The 4 phases

```
Phase 1  CRITIQUE   DB only, no enrichment     per_row.jsonl + report.md
Phase 2  SYNTHESIZE 1 Gemini Pro call           candidate_prompt.md + changelog
Phase 3  VERIFY     re-enrich + Flash N×        accuracy_report.md
         DEPLOY     patch ai_classifier.py      prompts/vN.md + versions.json
```

### Commands

```bash
# Smoke test first (3 rows, no Pro calls)
python -m backend.labeling.eval.eval_human_corrections \
    --source db --mode critique --run-id smoke --limit 3 --skip-meta

# Full pipeline
RUN=2026-$(date +%m-%d)_v1

python -m backend.labeling.eval.eval_human_corrections \
    --source db --mode critique --run-id $RUN

python -m backend.labeling.eval.eval_human_corrections \
    --mode synthesize --run-id $RUN

# Review diff before verifying
diff backend/labeling/prompts/current.md \
     backend/local/eval_results/$RUN/candidate_prompt.md

python -m backend.labeling.eval.eval_human_corrections \
    --source db --mode verify --run-id $RUN

# Deploy only after reviewing accuracy_report.md
python -m backend.labeling.eval.eval_human_corrections \
    --mode deploy --run-id $RUN
```

---

## Reading the accuracy report

`accuracy_report.md` in the run directory. Key dimensions:

| Dimension | What it means |
|-----------|--------------|
| Category exact | `usage_category` matches human correction |
| Name exact / partial | exact or ≥50% word overlap |
| Owner confirmed | `protocol_likely=True` when human confirmed owner |
| No-owner confirmed | `protocol_likely=False` when human confirmed no owner |
| Wins / Regressions | wrong→right / right→wrong vs original prompt |
| Confusion matrix | remaining wrong predictions: gt → predicted |
| Confidence tiers | category accuracy split high/mid/low |

**The 0% original baseline is expected** — rows are selected precisely because the AI was wrong on them.

**No regressions = safe to deploy.** Any regression (right→wrong) needs investigation before deploying.

---

## Debugging common failures

### Phase 1 produces no proposals
- Check `per_row.jsonl` exists and is non-empty
- Check `contract_label_review` has rows with human corrections (`human_usage_category`, `human_contract_name`, `gtp_no_owner_project`, etc.)
- Inspect rows: `python3 -c "import json; [print(json.dumps(r, indent=2)) for r in [json.loads(l) for l in open('backend/local/eval_results/<run-id>/per_row.jsonl')][:2]]"`

### Phase 2 returns unchanged prompt
- Proposals didn't meet thresholds: `confidence >= 0.6 AND count >= 2` OR `confidence >= 0.85`
- Check `candidate_prompt_changelog.md` for `skip_reason`
- Low proposal count = not enough ground-truth rows; need more human corrections in Airtable

### Phase 3 keeps re-fetching (slow)
- Cache miss on every contract → check `backend/local/eval_cache/` exists and has `.json` files
- Cache is populated on first run; subsequent runs are fast
- Force refresh: `--refresh` flag on `context_cache.py` CLI

### Owner accuracy shows 0% for new prompt
- Means `ai_owner_project_likely` wasn't computed — only happens with old `verify_per_row.jsonl` files
- Re-run Phase 3; `reclassify_row` now calls `_is_protocol_likely()` and attaches the field

### `UndefinedTable` error during eval
- `upsert_table` called with `public.<table>` prefix — pangres double-qualifies it
- Pass bare table name only (no `public.` prefix)

### Chain median DAA logged once per contract (slow DB)
- Fixed via `_chain_median_cache` in `automated_labeler.py` — per-process cache
- If still seeing repeated logs, check that `automated_labeler` module wasn't reloaded mid-run

---

## Key files

| File | Role |
|------|------|
| `eval_human_corrections.py` | Orchestrator — all phases + deploy |
| `prompt_synthesizer.py` | Phase 2 — aggregate proposals → Pro call → write candidate |
| `re_eval_prompt.py` | Meta-eval + synthesis prompt schemas (Pro model instructions) |
| `context_cache.py` | Phase 3 enrichment cache (Blockscout/Tenderly/GitHub) |
| `backend/labeling/prompts/versions.json` | Prompt version history with accuracy per version |
| `backend/labeling/prompts/current.md` | Live prompt (matches `_SYSTEM_INSTRUCTION`) |

## Key DB tables

| Table | What's in it |
|-------|-------------|
| `contract_label_features` | AI signals + classification per contract (Phase 1 source) |
| `contract_label_review` | Human corrections audit log (ground-truth source) |

---

## Prompt versioning

```
backend/labeling/prompts/
├── versions.json   ← {current: "v2", history: [{version, run_id, deployed_at, accuracy}]}
├── current.md      ← copy of live _SYSTEM_INSTRUCTION
├── v1.md
└── v2.md
```

Deploy auto-increments version, writes `vN.md`, updates `current.md` and `versions.json`.
To roll back: copy `vN.md` into `_SYSTEM_INSTRUCTION` in `ai_classifier.py` manually and update `versions.json`.

---

## Ground-truth filter

A row is evaluated only when `contract_label_review` has at least one of:
`human_contract_name`, `human_usage_category`, `gtp_owner_project_confirmed=True`, `gtp_no_owner_project=True`, `human_comment`

More human corrections in Airtable → better eval signal.
