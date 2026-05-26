# Classifier Self-Critique Eval — RL-Inspired 3-Phase Pipeline

Iteratively improves `_SYSTEM_INSTRUCTION` in `ai_classifier.py` by critiquing the classifier
against human corrections, synthesizing a better prompt, and verifying the improvement.

## How it works

```
Phase 1  CRITIQUE   DB only, no enrichment API    N × Pro calls   per_row.jsonl + report.md
Phase 2  SYNTHESIZE 1 Pro call                    1 Pro call      candidate_prompt.md
Phase 3  VERIFY     re-enrich + Flash             N × Flash       accuracy_report.md
         DEPLOY     patch ai_classifier.py        0 API calls     prompts/vN.md + versions.json
```

`contract_label_features` stores all decoded signals the meta-critique needs.
Critiquing stored AI output first (zero enrichment cost) produces ranked proposals.
Only after a candidate prompt exists does Phase 3 spend Blockscout/Tenderly/Flash credits.

## Files

| File | Role |
|------|------|
| `eval_human_corrections.py` | Orchestrator — all phases + deploy |
| `prompt_synthesizer.py` | Phase 2 — aggregate proposals → call Pro → write candidate prompt |
| `re_eval_prompt.py` | Meta-eval prompt + schema; synthesis prompt + schema |
| `context_cache.py` | On-chain enrichment cache for Phase 3 (Blockscout/Tenderly/GitHub) |

## Prompt versioning

Deployed prompts live in `backend/labeling/prompts/`:

```
backend/labeling/prompts/
├── versions.json    ← manifest: current pointer + history with accuracy per version
├── current.md       ← copy of whatever is live in ai_classifier.py
├── v1.md
├── v2.md
└── ...
```

`versions.json` entry per version:
```json
{
  "version": "v2",
  "run_id": "2026-05-26_v1",
  "deployed_at": "2026-05-26T10:00:00Z",
  "lines": 325,
  "accuracy": {
    "category_new": "18/31 (58.1%)",
    "name_exact_new": "10/45 (22.2%)",
    "wins_regressions": "Wins: 18 | Regressions: 1"
  }
}
```

## Quickstart

### Phase 1 — critique

```bash
# Smoke test: 3 rows, no Pro calls
python -m backend.labeling.eval.eval_human_corrections \
    --source db --mode critique --run-id smoke --limit 3 --skip-meta

# Full critique
python -m backend.labeling.eval.eval_human_corrections \
    --source db --mode critique --run-id $(date +%Y-%m-%d)_v1
```

Outputs to `backend/local/eval_results/<run-id>/`:
- `per_row.jsonl` — per-contract errors + proposals
- `report.md` — ranked error patterns + prompt-edit proposals

### Phase 2 — synthesize

```bash
python -m backend.labeling.eval.eval_human_corrections \
    --mode synthesize --run-id <run-id>
```

Outputs:
- `candidate_prompt.md` — complete revised `_SYSTEM_INSTRUCTION`
- `candidate_prompt_changelog.md` — what changed and why

Review the diff before Phase 3:
```bash
diff backend/labeling/prompts/current.md \
     backend/local/eval_results/<run-id>/candidate_prompt.md
```

### Phase 3 — verify

```bash
python -m backend.labeling.eval.eval_human_corrections \
    --source db --mode verify --run-id <run-id>
```

Output: `accuracy_report.md` — category/name/owner accuracy before vs after, confusion matrix,
name partial credit (word overlap ≥50%), confidence calibration by tier.

### Deploy

Once satisfied with the accuracy report:

```bash
python -m backend.labeling.eval.eval_human_corrections \
    --mode deploy --run-id <run-id>
```

- Patches `_SYSTEM_INSTRUCTION` in `ai_classifier.py`
- Writes `backend/labeling/prompts/vN.md`
- Updates `current.md` and `versions.json`
- No-ops if the prompt is already current

### Full pipeline in one command

```bash
python -m backend.labeling.eval.eval_human_corrections \
    --source db --mode full --run-id <run-id>
```

Runs Phases 1–3 sequentially. Deploy is a separate step by design — review
`accuracy_report.md` before committing to a new version.

## Accuracy report dimensions

| Dimension | What it measures |
|-----------|-----------------|
| Category exact | `usage_category` matches human correction |
| Name exact | `contract_name` case-insensitive exact match |
| Name partial | ≥50% word overlap between predicted and human name |
| Owner confirmed | `protocol_likely=True` when human confirmed an owner |
| No-owner confirmed | `protocol_likely=False` when human confirmed no owner |
| Confidence tiers | Category accuracy split by high/mid/low confidence |
| Wins / Regressions | Rows flipped right→wrong and wrong→right vs original |
| Confusion matrix | Ground truth → predicted for remaining wrong predictions |

## Ground-truth filter

A row is evaluated only when at least one of these is set in `contract_label_review`:

- `human_contract_name`
- `human_usage_category`
- `gtp_owner_project_confirmed=True` AND `owner_project_linked` non-empty
- `gtp_no_owner_project=True`
- `human_comment` non-empty

## Phase 2 proposal thresholds

Applied if: `confidence >= 0.6 AND count >= 2`
OR: `confidence >= 0.85` (single high-confidence proposal).
Conflicting proposals for the same section: highest confidence wins.

## Env required

- `GEMINI_API_KEY` — Pro for Phases 1–2, Flash for Phase 3
- `DB_*` — all phases (`contract_label_features`, `contract_label_review`, chain median DAA)
- `BLOCKSCOUT_API_KEYS`, `TENDERLY_ACCESS_KEY`, GitHub token — Phase 3 cache misses only

## Further reference

Debugging guide (phase failures, slow runs, owner scoring, common errors): `.claude/skills/classifier-eval-loop.md`
