"""Classifier self-critique eval loop — RL-inspired 3-phase pipeline.

Phases:
  1. CRITIQUE  (--mode critique)   DB-only, no enrichment API calls.
               Reconstructs raw signals from contract_label_features, runs meta-eval
               against stored AI output vs human corrections → per_row.jsonl + report.md

  2. SYNTHESIZE (--mode synthesize) One Pro call.
               Reads per_row.jsonl proposals, calls prompt_synthesizer → candidate_prompt.md

  3. VERIFY   (--mode verify)      Re-enrich + re-classify with candidate prompt.
               Scores category / name / owner accuracy before vs after → accuracy_report.md

  full        Runs all three phases sequentially.
  reclassify  Legacy: re-enrich → re-classify (current prompt) → meta-eval (original flow).

CLI:
  # Phase 1 — diagnose from stored features (no API cost):
  python -m backend.labeling.eval.eval_human_corrections \\
      --source db --mode critique --run-id 2026-05-19_v1 [--limit N] [--skip-meta]

  # Phase 2 — generate improved prompt:
  python -m backend.labeling.eval.eval_human_corrections \\
      --mode synthesize --run-id 2026-05-19_v1

  # Phase 3 — verify improvement:
  python -m backend.labeling.eval.eval_human_corrections \\
      --source db --mode verify --run-id 2026-05-19_v1

  # All at once:
  python -m backend.labeling.eval.eval_human_corrections \\
      --source db --mode full --run-id 2026-05-19_v1
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
from dotenv import load_dotenv

# ── Path setup ────────────────────────────────────────────────────────────────
_THIS_DIR = Path(__file__).resolve().parent
_LABELING_DIR = _THIS_DIR.parent
_BACKEND_DIR = _LABELING_DIR.parent
_REPO_ROOT = _BACKEND_DIR.parent
for _p in [str(_LABELING_DIR), str(_BACKEND_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
load_dotenv(dotenv_path=_BACKEND_DIR / ".env", override=False)
load_dotenv(dotenv_path=_REPO_ROOT / ".env", override=False)
load_dotenv(override=False)

from ai_classifier import classify_contract, _SYSTEM_INSTRUCTION, _get_gemini_client  # noqa: E402
from google.genai import types as genai_types  # noqa: E402

if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))
import context_cache as cc  # noqa: E402
import re_eval_prompt as rep  # noqa: E402

logger = logging.getLogger("eval.orchestrator")

META_MODEL = "gemini-2.5-pro"
RESULTS_DIR = _BACKEND_DIR / "local" / "eval_results"


# ── DB loader ────────────────────────────────────────────────────────────────

def load_rows_from_db() -> list[dict]:
    """Join contract_label_features (AI baseline + signals) with contract_label_review (human corrections).

    Fetches all feature columns so Phase 1 (critique) can reconstruct raw_signals without
    any enrichment API calls.
    """
    from src.db_connector import DbConnector
    from sqlalchemy import text as sa_text
    sql = sa_text("""
        SELECT '0x' || encode(f.address, 'hex')                                AS address,
               f.origin_key                                                    AS origin_key,
               -- AI output (baseline)
               f.ai_contract_name                                              AS ai_contract_name,
               f.ai_usage_category                                             AS ai_usage_category,
               f.ai_confidence                                                 AS confidence,
               f.ai_reasoning                                                  AS _comment,
               f.ai_owner_project_likely                                       AS is_protocol_contract_likely,
               f.ai_model                                                      AS _source,
               -- Human corrections
               r.gtp_contract_name                                             AS human_contract_name,
               r.gtp_usage_category                                            AS human_usage_category,
               r.gtp_owner_project                                             AS owner_project_linked,
               (r.gtp_owner_project IS NOT NULL)                               AS gtp_owner_project_confirmed,
               COALESCE(r.gtp_no_owner_project, false)                         AS gtp_no_owner_project,
               ''::text                                                        AS temp_owner_project,
               ''::text                                                        AS human_comment,
               (r.id IS NOT NULL)                                              AS approve,
               -- Blockscout signals
               f.blockscout_name,
               f.is_verified,
               f.is_proxy,
               CASE WHEN f.impl_address IS NOT NULL
                    THEN '0x' || encode(f.impl_address, 'hex') END             AS impl_address,
               f.impl_name,
               f.has_github_repo,
               -- Activity metrics
               f.txcount,
               f.avg_daa,
               f.success_rate,
               f.rel_cost,
               f.metric_day_range,
               -- Trace-derived signals
               f.novel_tokens,
               f.common_tokens,
               f.bs_token_transfers,
               f.matched_routers,
               f.matched_dex_pools,
               f.calls_into_dex_pool_swap,
               f.matched_lending,
               f.matched_staking,
               f.matched_bridges,
               f.matched_oracle_fns,
               f.delegates_to,
               f.raw_delegate,
               f.all_traces_empty,
               f.traces_only_raw_opcodes,
               f.named_contracts_in_traces,
               f.caller_diversity_pct,
               f.caller_diversity_label,
               f.unique_callers,
               -- Log signals
               f.log_has_access_control,
               f.log_has_token_transfers,
               f.log_has_swaps,
               f.log_has_bridge_events,
               f.log_has_erc4337,
               f.log_has_staking,
               f.log_has_governance,
               f.log_category_hints,
               f.log_top_events
        FROM public.contract_label_features f
        LEFT JOIN LATERAL (
            SELECT * FROM public.contract_label_review r2
            WHERE r2.address = f.address AND r2.origin_key = f.origin_key
            ORDER BY r2.reviewed_at DESC
            LIMIT 1
        ) r ON true
        WHERE r.id IS NOT NULL
          AND (r.name_was_changed
               OR r.category_was_changed
               OR r.owner_was_assigned
               OR COALESCE(r.gtp_no_owner_project, false))
    """)
    db = DbConnector()
    with db.engine.begin() as conn:
        rows = [dict(m) for m in conn.execute(sql).mappings().all()]
    logger.info(f"loaded {len(rows)} ground-truth rows from DB")
    return rows


# ── Filtering helpers ────────────────────────────────────────────────────────

def has_human_correction(row: dict) -> bool:
    """A row counts as ground-truth iff a human supplied at least one signal."""
    if (row.get("human_contract_name") or "").strip():
        return True
    if (row.get("human_usage_category") or "").strip():
        return True
    if row.get("gtp_owner_project_confirmed") and (row.get("owner_project_linked") or "").strip():
        return True
    if bool(row.get("gtp_no_owner_project")):
        return True
    if (row.get("temp_owner_project") or "").strip():
        return True
    if (row.get("human_comment") or "").strip():
        return True
    return False


def ground_truth_block(row: dict) -> dict:
    return {
        "human_contract_name": row.get("human_contract_name") or "",
        "human_usage_category": row.get("human_usage_category") or "",
        "owner_project_linked": row.get("owner_project_linked") or "",
        "gtp_owner_project_confirmed": bool(row.get("gtp_owner_project_confirmed")),
        "gtp_no_owner_project": bool(row.get("gtp_no_owner_project")),
        "temp_owner_project": row.get("temp_owner_project") or "",
        "human_comment": row.get("human_comment") or "",
        "approve": bool(row.get("approve")),
    }


def ai_original_block(row: dict) -> dict:
    return {
        "contract_name": row.get("ai_contract_name") or "",
        "usage_category": row.get("ai_usage_category") or "",
        "confidence": row.get("confidence"),
        "reasoning": row.get("_comment") or "",
        "is_protocol_contract_likely": bool(row.get("is_protocol_contract_likely")),
        "_source": row.get("_source") or "",
    }


# ── Phase 1: reconstruct raw_signals from DB columns ─────────────────────────

def reconstruct_raw_signals_from_features(
    row: dict,
    chain_median_map: dict[str, float],
) -> dict:
    """Build raw_signals from stored contract_label_features — no API calls.

    Produces a superset of _raw_signals_summary(): includes all old-format fields
    for backward compat with META_SYSTEM_INSTRUCTION PLUS richer decoded signal groups.
    """
    def _lst(v):
        if v is None:
            return []
        return list(v) if not isinstance(v, list) else v

    transfers = _lst(row.get("bs_token_transfers"))
    named = _lst(row.get("named_contracts_in_traces"))

    # Approximate trace/log counts from stored signals
    all_empty = bool(row.get("all_traces_empty"))
    unique_callers = row.get("unique_callers") or 0
    traces_count_approx = 0 if all_empty else (unique_callers or 1)
    log_flags = [
        row.get("log_has_access_control"), row.get("log_has_token_transfers"),
        row.get("log_has_swaps"), row.get("log_has_bridge_events"),
        row.get("log_has_erc4337"), row.get("log_has_staking"), row.get("log_has_governance"),
    ]
    logs_count_approx = sum(1 for f in log_flags if f)

    return {
        # ── Old-format fields (backward compat with META_SYSTEM_INSTRUCTION) ──
        "blockscout": {
            "contract_name": row.get("blockscout_name"),
            "is_verified": row.get("is_verified"),
            "is_proxy": row.get("is_proxy"),
            "impl_address": row.get("impl_address"),
            "impl_name": row.get("impl_name"),
        },
        "github": {
            "has_valid_repo": row.get("has_github_repo"),
        },
        "metrics": {
            "txcount": row.get("txcount"),
            "avg_daa": row.get("avg_daa"),
            "success_rate": row.get("success_rate"),
            "rel_cost": row.get("rel_cost"),
            "day_range": row.get("metric_day_range"),
            "chain_median_daa": chain_median_map.get(row.get("origin_key", ""), 0.0),
        },
        "traces_count": traces_count_approx,
        "address_logs_count": logs_count_approx,
        "token_transfers_count": len(transfers),
        "named_contracts_in_traces_sample": named[:20],
        "token_transfers_sample": [
            {"name": t.get("token_name"), "symbol": t.get("token_symbol")}
            for t in transfers[:10]
        ],
        # ── Richer decoded signals (unavailable in legacy _raw_signals_summary) ──
        "trace_signals": {
            "matched_routers": _lst(row.get("matched_routers")),
            "matched_dex_pools": _lst(row.get("matched_dex_pools")),
            "calls_into_dex_pool_swap": bool(row.get("calls_into_dex_pool_swap")),
            "matched_lending": _lst(row.get("matched_lending")),
            "matched_staking": _lst(row.get("matched_staking")),
            "matched_bridges": _lst(row.get("matched_bridges")),
            "matched_oracle_fns": _lst(row.get("matched_oracle_fns")),
            "novel_tokens": _lst(row.get("novel_tokens")),
            "common_tokens": _lst(row.get("common_tokens")),
            "all_traces_empty": all_empty,
            "traces_only_raw_opcodes": bool(row.get("traces_only_raw_opcodes")),
            "delegates_to": row.get("delegates_to"),
            "raw_delegate": bool(row.get("raw_delegate")),
            "caller_diversity_pct": row.get("caller_diversity_pct"),
            "caller_diversity_label": row.get("caller_diversity_label"),
            "unique_callers": unique_callers,
        },
        "log_signals": {
            "log_has_access_control": bool(row.get("log_has_access_control")),
            "log_has_token_transfers": bool(row.get("log_has_token_transfers")),
            "log_has_swaps": bool(row.get("log_has_swaps")),
            "log_has_bridge_events": bool(row.get("log_has_bridge_events")),
            "log_has_erc4337": bool(row.get("log_has_erc4337")),
            "log_has_staking": bool(row.get("log_has_staking")),
            "log_has_governance": bool(row.get("log_has_governance")),
            "log_category_hints": _lst(row.get("log_category_hints")),
            "log_top_events": row.get("log_top_events") or {},
        },
    }


# ── Re-classify (used by legacy reclassify mode and Phase 3 verify) ──────────

async def reclassify_row(
    row: dict,
    session: aiohttp.ClientSession,
    enrich_sem: asyncio.Semaphore,
    github_sem: asyncio.Semaphore,
    classify_sem: asyncio.Semaphore,
    refresh: bool = False,
) -> tuple[dict, dict]:
    """Returns (ai_rerun, ctx). ai_rerun is the classifier's full return dict."""
    from automated_labeler import _is_protocol_likely
    address = row["address"]
    origin_key = row["origin_key"]
    ctx = await cc.build_or_load(
        address, origin_key,
        dump_row=row, refresh=refresh,
        session=session, enrich_sem=enrich_sem, github_sem=github_sem,
    )
    async with classify_sem:
        result = await classify_contract(
            address=address,
            origin_key=origin_key,
            metrics=ctx.get("metrics", {}),
            blockscout=ctx.get("blockscout", {}),
            github=ctx.get("github", {}),
            traces=ctx.get("traces", []),
            session=session,
            address_logs=ctx.get("address_logs", []),
            token_transfers=ctx.get("token_transfers", []),
        )
    log_signals = result.get("log_signals") or ctx.get("log_signals") or {}
    result["ai_owner_project_likely"] = _is_protocol_likely(
        result, ctx.get("blockscout", {}), log_signals, ctx.get("metrics", {})
    )
    return result, ctx


# ── Meta-eval ────────────────────────────────────────────────────────────────

def _raw_signals_summary(ctx: dict) -> dict:
    """Build raw_signals from a live enrichment context (legacy / verify mode)."""
    bs = ctx.get("blockscout", {}) or {}
    gh = ctx.get("github", {}) or {}
    metrics = ctx.get("metrics", {}) or {}
    traces = ctx.get("traces", []) or []
    logs = ctx.get("address_logs", []) or []
    transfers = ctx.get("token_transfers", []) or []

    named_in_traces = sorted({
        c.get("contract", "") for t in traces for c in (t.get("protocol_calls") or [])
        if c.get("contract")
    })
    return {
        "blockscout": {
            "contract_name": bs.get("contract_name"),
            "is_verified": bs.get("is_verified"),
            "is_proxy": bs.get("is_proxy"),
            "impl_address": bs.get("impl_address"),
        },
        "github": {
            "has_valid_repo": gh.get("has_valid_repo"),
            "repo_url": gh.get("repo_url", ""),
        },
        "metrics": {
            "txcount": metrics.get("txcount"),
            "avg_daa": metrics.get("avg_daa"),
            "success_rate": metrics.get("success_rate"),
            "rel_cost": metrics.get("rel_cost"),
            "day_range": metrics.get("day_range"),
            "chain_median_daa": metrics.get("chain_median_daa"),
        },
        "traces_count": len(traces),
        "address_logs_count": len(logs),
        "token_transfers_count": len(transfers),
        "named_contracts_in_traces_sample": named_in_traces[:20],
        "token_transfers_sample": [
            {"name": tt.get("token_name"), "symbol": tt.get("token_symbol")}
            for tt in transfers[:10]
        ],
    }


def _meta_user_message(payload: dict, prompt_override: str | None = None) -> str:
    prompt = prompt_override if prompt_override is not None else _SYSTEM_INSTRUCTION
    return (
        "CURRENT_SYSTEM_PROMPT:\n"
        "<<<\n" + prompt + "\n>>>\n\n"
        "GROUND_TRUTH:\n" + json.dumps(payload["ground_truth"], indent=2) + "\n\n"
        "AI_RERUN:\n" + json.dumps(payload["ai_rerun"], indent=2, default=str) + "\n\n"
        "AI_ORIGINAL:\n" + json.dumps(payload["ai_original"], indent=2, default=str) + "\n\n"
        "RAW_SIGNALS:\n" + json.dumps(payload["raw_signals"], indent=2, default=str)
    )


async def meta_eval(
    payload: dict,
    sem: asyncio.Semaphore,
    prompt_override: str | None = None,
) -> dict:
    client = _get_gemini_client()
    user_msg = _meta_user_message(payload, prompt_override=prompt_override)

    def _call():
        return client.models.generate_content(
            model=META_MODEL,
            contents=user_msg,
            config=genai_types.GenerateContentConfig(
                system_instruction=rep.META_SYSTEM_INSTRUCTION,
                response_mime_type="application/json",
                response_schema=rep.META_RESPONSE_SCHEMA,
                temperature=0.2,
                max_output_tokens=4096,
            ),
        )

    async with sem:
        loop = asyncio.get_event_loop()
        try:
            response = await loop.run_in_executor(None, _call)
        except Exception as e:
            logger.error(f"meta_eval API call failed: {e}")
            return {"errors": [], "prompt_edit_proposals": [], "severity": "low",
                    "novel_pattern": False, "_meta_error": str(e)}

    raw = response.text or ""
    cleaned = re.sub(r",\s*([}\]])", r"\1", raw)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.warning(f"meta_eval JSON parse failed: {e} | raw: {raw[:200]!r}")
        return {"errors": [], "prompt_edit_proposals": [], "severity": "low",
                "novel_pattern": False, "_meta_error": "json_parse_failed",
                "_meta_raw": raw[:1000]}


# ── Aggregation ──────────────────────────────────────────────────────────────

def aggregate(per_row_path: Path, out_md: Path, run_id: str) -> None:
    rows = [json.loads(line) for line in per_row_path.read_text().splitlines() if line.strip()]
    n = len(rows)

    sev_counts = Counter(r.get("meta_eval", {}).get("severity", "low") for r in rows)
    novel_rows = [r for r in rows if r.get("meta_eval", {}).get("novel_pattern")]
    rows_with_errors = [r for r in rows if r.get("meta_eval", {}).get("errors")]

    error_buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        for e in r.get("meta_eval", {}).get("errors", []):
            key = (e.get("category", "?"), (e.get("root_cause_in_prompt") or "?")[:80])
            error_buckets[key].append({
                "address": r["address"],
                "origin_key": r["origin_key"],
                "what_went_wrong": e.get("what_went_wrong", ""),
                "evidence": e.get("evidence_from_signals", ""),
                "severity": r.get("meta_eval", {}).get("severity", "low"),
            })

    edit_buckets: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        for p in r.get("meta_eval", {}).get("prompt_edit_proposals", []):
            edit_buckets[(p.get("target_section") or "?")[:80]].append({
                "address": r["address"],
                "proposed_change": p.get("proposed_change", ""),
                "rationale": p.get("rationale", ""),
                "confidence": float(p.get("confidence", 0)),
            })

    sev_weight = {"low": 1, "medium": 2, "high": 3}

    lines: list[str] = []
    lines.append(f"# Classifier eval — run `{run_id}`")
    lines.append("")
    lines.append(f"- Rows evaluated: **{n}**")
    if n:
        lines.append(f"- Rows with at least one error: **{len(rows_with_errors)}** ({len(rows_with_errors) / n:.0%})")
    lines.append(f"- Severity: high={sev_counts.get('high',0)}  medium={sev_counts.get('medium',0)}  low={sev_counts.get('low',0)}")
    lines.append(f"- Novel-pattern rows: **{len(novel_rows)}**")
    lines.append("")

    lines.append("## 1. Recurring error patterns")
    lines.append("")
    sorted_buckets = sorted(
        error_buckets.items(),
        key=lambda kv: (
            -sum(sev_weight.get(x["severity"], 1) for x in kv[1]),
            -len(kv[1]),
        ),
    )
    if not sorted_buckets:
        lines.append("_No errors recorded._")
    for (cat, root), items in sorted_buckets:
        weight = sum(sev_weight.get(x["severity"], 1) for x in items)
        lines.append(f"### {cat} — {root}")
        lines.append(f"- count={len(items)}  severity_weight={weight}")
        for ex in items[:3]:
            lines.append(f"  - `{ex['origin_key']}/{ex['address']}` — {ex['what_went_wrong']}")
            if ex["evidence"]:
                lines.append(f"    - evidence: {ex['evidence']}")
        lines.append("")

    lines.append("## 2. Ranked prompt-edit proposals")
    lines.append("")
    sorted_edits = sorted(
        edit_buckets.items(),
        key=lambda kv: -(len(kv[1]) * (sum(x["confidence"] for x in kv[1]) / max(len(kv[1]), 1))),
    )
    if not sorted_edits:
        lines.append("_No proposals._")
    for section, props in sorted_edits:
        avg_conf = sum(p["confidence"] for p in props) / max(len(props), 1)
        lines.append(f"### `{section}`  (proposals: {len(props)}, avg_conf: {avg_conf:.2f})")
        seen: set[str] = set()
        for p in sorted(props, key=lambda x: -x["confidence"]):
            key = p["proposed_change"][:120]
            if key in seen:
                continue
            seen.add(key)
            lines.append(f"- **conf={p['confidence']:.2f}** — {p['proposed_change']}")
            if p["rationale"]:
                lines.append(f"  - rationale: {p['rationale']}")
            if len(seen) >= 3:
                break
        lines.append("")

    lines.append("## 3. Novel patterns (not P1–P7)")
    lines.append("")
    if not novel_rows:
        lines.append("_None._")
    for r in novel_rows[:20]:
        gt = r.get("ground_truth", {})
        ai = r.get("ai_rerun", {})
        lines.append(
            f"- `{r['origin_key']}/{r['address']}` — ai_name={ai.get('contract_name')!r} "
            f"ai_cat={ai.get('usage_category')!r} | human_name={gt.get('human_contract_name')!r} "
            f"human_cat={gt.get('human_usage_category')!r}"
        )
        for e in r.get("meta_eval", {}).get("errors", [])[:2]:
            lines.append(f"  - {e.get('category')}: {e.get('what_went_wrong')}")
    lines.append("")

    lines.append("## 4. Per-row diff (AI original vs rerun vs human)")
    lines.append("")
    lines.append("| origin/addr | ai_orig name | ai_rerun name | human name | ai_orig cat | ai_rerun cat | human cat | severity |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for r in rows:
        ao = r.get("ai_original", {})
        ar = r.get("ai_rerun", {})
        gt = r.get("ground_truth", {})
        sev = r.get("meta_eval", {}).get("severity", "low")
        lines.append(
            f"| `{r['origin_key']}/{r['address'][:10]}…` "
            f"| {ao.get('contract_name','')} | {ar.get('contract_name','')} | {gt.get('human_contract_name','')} "
            f"| {ao.get('usage_category','')} | {ar.get('usage_category','')} | {gt.get('human_usage_category','')} "
            f"| {sev} |"
        )

    out_md.write_text("\n".join(lines) + "\n")
    logger.info(f"wrote aggregated report → {out_md}")


# ── Phase 1: critique ────────────────────────────────────────────────────────

async def run_critique_phase(
    filtered: list[dict],
    run_id: str,
    meta_concurrency: int = 3,
    skip_meta: bool = False,
) -> Path:
    """Phase 1: meta-critique from stored features — zero enrichment API calls.

    Uses reconstruct_raw_signals_from_features() to build a richer signal summary
    than the legacy reclassify flow, then calls meta_eval against the stored AI output.
    Returns path to per_row.jsonl.
    """
    from automated_labeler import fetch_chain_median_daa

    out_dir = RESULTS_DIR / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    per_row_path = out_dir / "per_row.jsonl"
    report_path = out_dir / "report.md"

    # One DB query for chain median DAA across all needed origin_keys
    origin_keys = list({r["origin_key"] for r in filtered})
    chain_median_map: dict[str, float] = {}
    try:
        chain_median_map = fetch_chain_median_daa(origin_keys)
    except Exception as e:
        logger.warning(f"chain_median_daa fetch failed: {e} — using 0.0 for all chains")

    meta_sem = asyncio.Semaphore(meta_concurrency)
    written = 0

    async def _critique_one(row: dict) -> Optional[dict]:
        ai_orig = ai_original_block(row)
        raw_signals = reconstruct_raw_signals_from_features(row, chain_median_map)
        payload = {
            "address": row["address"],
            "origin_key": row["origin_key"],
            "ground_truth": ground_truth_block(row),
            "ai_original": ai_orig,
            "ai_rerun": ai_orig,  # no rerun in critique phase — critique the stored output
            "raw_signals": raw_signals,
            "phase": "critique",
        }
        if not skip_meta:
            payload["meta_eval"] = await meta_eval(payload, meta_sem)
        else:
            payload["meta_eval"] = {
                "errors": [], "prompt_edit_proposals": [],
                "severity": "low", "novel_pattern": False, "_skipped": True,
            }
        payload["ts"] = datetime.now(timezone.utc).isoformat()
        return payload

    with per_row_path.open("w") as out_f:
        results = await asyncio.gather(*(_critique_one(r) for r in filtered))
        for res in results:
            if res is None:
                continue
            out_f.write(json.dumps(res, default=str) + "\n")
            written += 1

    logger.info(f"[critique] wrote {written} rows → {per_row_path}")
    aggregate(per_row_path, report_path, run_id)
    return per_row_path


# ── Phase 3: verify ──────────────────────────────────────────────────────────

def _name_word_overlap(a: str, b: str) -> float:
    """Normalized word overlap between two contract names (case-insensitive)."""
    wa = set(a.lower().split())
    wb = set(b.lower().split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / max(len(wa), len(wb))


def score_accuracy(rows: list[dict]) -> dict:
    """Compare ai_original and ai_rerun against ground_truth per correction dimension."""
    n = len(rows)
    cat_orig = cat_new = cat_total = 0
    cat_wins = cat_regressions = 0
    cat_confusion: Counter = Counter()  # (gt, predicted_new) for wrong new predictions

    name_orig = name_new = name_total = 0
    name_partial_orig = name_partial_new = 0  # word-overlap >= 0.5

    owner_orig = owner_new = owner_total = 0
    no_owner_orig = no_owner_new = no_owner_total = 0  # gtp_no_owner_project cases

    conf_buckets: dict[str, list[int]] = {"high": [], "mid": [], "low": []}

    for r in rows:
        gt = r.get("ground_truth", {})
        ao = r.get("ai_original", {})
        ar = r.get("ai_rerun", {})

        # ── Category ──────────────────────────────────────────────────────────
        human_cat = (gt.get("human_usage_category") or "").strip()
        if human_cat:
            cat_total += 1
            orig_right = ao.get("usage_category") == human_cat
            new_right = ar.get("usage_category") == human_cat
            if orig_right:
                cat_orig += 1
            if new_right:
                cat_new += 1
            if not orig_right and new_right:
                cat_wins += 1
            if orig_right and not new_right:
                cat_regressions += 1
            if not new_right:
                cat_confusion[(human_cat, ar.get("usage_category", "?"))] += 1

            # Confidence calibration on new predictions
            conf = float(ar.get("confidence") or 0)
            bucket = "high" if conf >= 0.8 else ("mid" if conf >= 0.5 else "low")
            conf_buckets[bucket].append(1 if new_right else 0)

        # ── Name ──────────────────────────────────────────────────────────────
        human_name = (gt.get("human_contract_name") or "").strip()
        if human_name:
            name_total += 1
            orig_name = (ao.get("contract_name") or "").lower()
            new_name = (ar.get("contract_name") or "").lower()
            gt_name = human_name.lower()
            if orig_name == gt_name:
                name_orig += 1
            if new_name == gt_name:
                name_new += 1
            if _name_word_overlap(orig_name, gt_name) >= 0.5:
                name_partial_orig += 1
            if _name_word_overlap(new_name, gt_name) >= 0.5:
                name_partial_new += 1

        # ── Owner: confirmed owner ─────────────────────────────────────────────
        if gt.get("gtp_owner_project_confirmed") and (gt.get("owner_project_linked") or "").strip():
            owner_total += 1
            if bool(ao.get("is_protocol_contract_likely")):
                owner_orig += 1
            if bool(ar.get("ai_owner_project_likely")):
                owner_new += 1

        # ── Owner: confirmed NO owner ──────────────────────────────────────────
        if bool(gt.get("gtp_no_owner_project")):
            no_owner_total += 1
            # Correct = model did NOT flag as protocol_likely
            if not bool(ao.get("is_protocol_contract_likely")):
                no_owner_orig += 1
            if not bool(ar.get("ai_owner_project_likely")):
                no_owner_new += 1

    def _pct(num: int, den: int) -> Optional[float]:
        return round(num / den * 100, 1) if den else None

    def _conf_acc(hits: list[int]) -> Optional[float]:
        return round(sum(hits) / len(hits) * 100, 1) if hits else None

    return {
        "n": n,
        "category": {
            "total_corrected": cat_total,
            "original_correct": cat_orig,
            "new_correct": cat_new,
            "original_pct": _pct(cat_orig, cat_total),
            "new_pct": _pct(cat_new, cat_total),
            "wins": cat_wins,
            "regressions": cat_regressions,
            "confusion": [
                {"gt": gt, "predicted": pred, "count": cnt}
                for (gt, pred), cnt in cat_confusion.most_common(15)
            ],
        },
        "name": {
            "total_corrected": name_total,
            "original_correct": name_orig,
            "new_correct": name_new,
            "original_pct": _pct(name_orig, name_total),
            "new_pct": _pct(name_new, name_total),
            "partial_original_correct": name_partial_orig,
            "partial_new_correct": name_partial_new,
            "partial_original_pct": _pct(name_partial_orig, name_total),
            "partial_new_pct": _pct(name_partial_new, name_total),
        },
        "owner_confirmed": {
            "total": owner_total,
            "original_correct": owner_orig,
            "new_correct": owner_new,
            "original_pct": _pct(owner_orig, owner_total),
            "new_pct": _pct(owner_new, owner_total),
        },
        "no_owner_confirmed": {
            "total": no_owner_total,
            "original_correct": no_owner_orig,
            "new_correct": no_owner_new,
            "original_pct": _pct(no_owner_orig, no_owner_total),
            "new_pct": _pct(no_owner_new, no_owner_total),
        },
        "confidence": {
            "high_acc": _conf_acc(conf_buckets["high"]),
            "high_n": len(conf_buckets["high"]),
            "mid_acc": _conf_acc(conf_buckets["mid"]),
            "mid_n": len(conf_buckets["mid"]),
            "low_acc": _conf_acc(conf_buckets["low"]),
            "low_n": len(conf_buckets["low"]),
        },
    }


def _write_accuracy_report(
    accuracy: dict,
    path: Path,
    run_id: str,
    candidate_prompt_path: Path,
) -> None:
    def _delta(orig_pct, new_pct) -> str:
        if orig_pct is None or new_pct is None:
            return "n/a"
        d = new_pct - orig_pct
        return f"+{d:.1f}%" if d >= 0 else f"{d:.1f}%"

    cat = accuracy["category"]
    name = accuracy["name"]
    oc = accuracy["owner_confirmed"]
    noc = accuracy["no_owner_confirmed"]
    conf = accuracy["confidence"]

    lines = [
        f"# Accuracy Report — run `{run_id}`",
        "",
        f"Candidate prompt: `{candidate_prompt_path}`",
        f"Rows verified: **{accuracy['n']}** (human-corrected subset — AI was wrong on all of these originally)",
        "",
        "## Category accuracy",
        "",
        f"| | Original | New prompt | Delta |",
        "|---|---|---|---|",
        f"| Correct | {cat['original_correct']}/{cat['total_corrected']} ({cat['original_pct']}%) | {cat['new_correct']}/{cat['total_corrected']} ({cat['new_pct']}%) | {_delta(cat['original_pct'], cat['new_pct'])} |",
        "",
        f"**Wins** (wrong→right): {cat['wins']}  |  **Regressions** (right→wrong): {cat['regressions']}",
        "",
    ]

    if cat.get("confusion"):
        lines += [
            "### Remaining errors — confusion (ground truth → predicted)",
            "",
            "| Ground truth | Predicted | Count |",
            "|---|---|---|",
        ]
        for entry in cat["confusion"]:
            lines.append(f"| {entry['gt']} | {entry['predicted']} | {entry['count']} |")
        lines.append("")

    lines += [
        "## Name accuracy",
        "",
        "| Match type | Original | New prompt | Delta |",
        "|---|---|---|---|",
        f"| Exact | {name['original_correct']}/{name['total_corrected']} ({name['original_pct']}%) | {name['new_correct']}/{name['total_corrected']} ({name['new_pct']}%) | {_delta(name['original_pct'], name['new_pct'])} |",
        f"| Partial (≥50% word overlap) | {name['partial_original_correct']}/{name['total_corrected']} ({name['partial_original_pct']}%) | {name['partial_new_correct']}/{name['total_corrected']} ({name['partial_new_pct']}%) | {_delta(name['partial_original_pct'], name['partial_new_pct'])} |",
        "",
        "## Owner / protocol detection",
        "",
        "| Case | Original | New prompt | Delta |",
        "|---|---|---|---|",
    ]

    if oc["total"]:
        lines.append(
            f"| Confirmed owner → protocol_likely=True | {oc['original_correct']}/{oc['total']} ({oc['original_pct']}%) | {oc['new_correct']}/{oc['total']} ({oc['new_pct']}%) | {_delta(oc['original_pct'], oc['new_pct'])} |"
        )
    if noc["total"]:
        lines.append(
            f"| Confirmed no-owner → protocol_likely=False | {noc['original_correct']}/{noc['total']} ({noc['original_pct']}%) | {noc['new_correct']}/{noc['total']} ({noc['new_pct']}%) | {_delta(noc['original_pct'], noc['new_pct'])} |"
        )

    lines += [
        "",
        "## Confidence calibration (new prompt, category accuracy by tier)",
        "",
        "| Confidence tier | Rows | Category accuracy |",
        "|---|---|---|",
        f"| High (≥0.8) | {conf['high_n']} | {conf['high_acc']}% |",
        f"| Mid (0.5–0.8) | {conf['mid_n']} | {conf['mid_acc']}% |",
        f"| Low (<0.5) | {conf['low_n']} | {conf['low_acc']}% |",
        "",
    ]

    path.write_text("\n".join(lines) + "\n")
    logger.info(f"wrote accuracy report → {path}")


async def run_verify_phase(
    filtered: list[dict],
    run_id: str,
    candidate_prompt_path: Path,
    classify_concurrency: int = 3,
    meta_concurrency: int = 3,
    refresh: bool = False,
) -> None:
    """Phase 3: re-enrich + re-classify with the candidate prompt, score accuracy vs original."""
    import ai_classifier as _ai_cls

    candidate_prompt = candidate_prompt_path.read_text().strip()
    original_prompt = _ai_cls._SYSTEM_INSTRUCTION

    out_dir = RESULTS_DIR / run_id
    verify_jsonl = out_dir / "verify_per_row.jsonl"
    accuracy_path = out_dir / "accuracy_report.md"

    # Load Phase 1 critique results for per-row baseline severity
    critique_by_key: dict[str, dict] = {}
    critique_path = out_dir / "per_row.jsonl"
    if critique_path.exists():
        for line in critique_path.read_text().splitlines():
            if line.strip():
                r = json.loads(line)
                critique_by_key[f"{r['origin_key']}/{r['address']}"] = r

    try:
        from src.db_connector import DbConnector
        from automated_labeler import _load_chain_config
        _load_chain_config(DbConnector().engine)
    except Exception as e:
        logger.warning(f"chain config load skipped: {e}")

    # Override classifier prompt for this phase (all tasks use the same new prompt)
    _ai_cls._SYSTEM_INSTRUCTION = candidate_prompt
    logger.info(f"[verify] classifier prompt overridden with {candidate_prompt_path}")

    all_results: list[dict] = []
    written = 0
    total = len(filtered)
    completed = 0

    try:
        connector = aiohttp.TCPConnector(ssl=cc._ssl_ctx(), limit=20)
        enrich_sem = asyncio.Semaphore(3)
        github_sem = asyncio.Semaphore(2)
        classify_sem = asyncio.Semaphore(classify_concurrency)
        meta_sem = asyncio.Semaphore(meta_concurrency)

        async def _verify_one(row: dict) -> Optional[dict]:
            nonlocal completed
            addr = row.get('address', '?')
            ok = row.get('origin_key', '?')
            try:
                ai_rerun, ctx = await reclassify_row(
                    row, session, enrich_sem, github_sem, classify_sem, refresh=refresh
                )
            except Exception as e:
                completed += 1
                logger.error(f"[verify] [{completed}/{total}] FAILED {ok}/{addr}: {e}")
                return None

            key = f"{row['origin_key']}/{row['address']}"
            orig_critique = critique_by_key.get(key, {})
            raw = _raw_signals_summary(ctx)

            payload = {
                "address": row["address"],
                "origin_key": row["origin_key"],
                "ground_truth": ground_truth_block(row),
                "ai_original": ai_original_block(row),
                "ai_rerun": ai_rerun,
                "raw_signals": raw,
            }
            result = {
                **payload,
                "meta_eval_original": orig_critique.get("meta_eval", {}),
                # meta_eval now critiques the rerun against the NEW prompt
                "meta_eval": await meta_eval(payload, meta_sem,
                                             prompt_override=candidate_prompt),
                "phase": "verify",
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            completed += 1
            cat = ai_rerun.get("usage_category", "?")
            gt_cat = payload["ground_truth"].get("human_usage_category") or payload["ground_truth"].get("usage_category", "?")
            match = "✓" if cat == gt_cat else "✗"
            logger.info(f"[verify] [{completed}/{total}] {match} {ok}/{addr[:10]}… → {cat} (gt={gt_cat})")
            return result

        with verify_jsonl.open("w") as out_f:
            async with aiohttp.ClientSession(connector=connector) as session:
                logger.info(f"[verify] starting {total} rows (concurrency: enrich={enrich_sem._value} classify={classify_sem._value})")
                results = await asyncio.gather(*(_verify_one(r) for r in filtered))
                for res in results:
                    if res is None:
                        continue
                    out_f.write(json.dumps(res, default=str) + "\n")
                    written += 1
                    all_results.append(res)

    finally:
        _ai_cls._SYSTEM_INSTRUCTION = original_prompt
        logger.info("[verify] classifier prompt restored to original")

    logger.info(f"[verify] wrote {written} rows → {verify_jsonl}")
    accuracy = score_accuracy(all_results)
    _write_accuracy_report(accuracy, accuracy_path, run_id, candidate_prompt_path)


# ── Legacy: reclassify mode (original pipeline) ───────────────────────────────

async def run_reclassify(
    dump_path: Path,
    run_id: str,
    limit: int = 0,
    skip_meta: bool = False,
    refresh: bool = False,
    classify_concurrency: int = 3,
    meta_concurrency: int = 3,
    source: str = "json",
) -> None:
    """Original pipeline: re-enrich → re-classify (current prompt) → meta-eval."""
    if source == "db":
        rows = load_rows_from_db()
    else:
        rows = json.loads(dump_path.read_text())
    filtered = [r for r in rows if r.get("address") and r.get("origin_key") and has_human_correction(r)]
    if limit:
        filtered = filtered[:limit]
    logger.info(f"[reclassify] {len(filtered)}/{len(rows)} rows (limit={limit or 'none'})")

    out_dir = RESULTS_DIR / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    per_row_path = out_dir / "per_row.jsonl"
    report_path = out_dir / "report.md"

    try:
        from src.db_connector import DbConnector
        from automated_labeler import _load_chain_config
        _load_chain_config(DbConnector().engine)
    except Exception as e:
        logger.warning(f"chain config load skipped: {e}")

    connector = aiohttp.TCPConnector(ssl=cc._ssl_ctx(), limit=20)
    enrich_sem = asyncio.Semaphore(3)
    github_sem = asyncio.Semaphore(2)
    classify_sem = asyncio.Semaphore(classify_concurrency)
    meta_sem = asyncio.Semaphore(meta_concurrency)

    written = 0
    with per_row_path.open("w") as out_f:
        async with aiohttp.ClientSession(connector=connector) as session:
            async def _process(row: dict) -> Optional[dict]:
                try:
                    ai_rerun, ctx = await reclassify_row(
                        row, session, enrich_sem, github_sem, classify_sem, refresh=refresh
                    )
                except Exception as e:
                    logger.error(f"reclassify failed for {row.get('address')}: {e}")
                    return None
                payload = {
                    "address": row["address"],
                    "origin_key": row["origin_key"],
                    "ground_truth": ground_truth_block(row),
                    "ai_original": ai_original_block(row),
                    "ai_rerun": ai_rerun,
                    "raw_signals": _raw_signals_summary(ctx),
                    "phase": "reclassify",
                }
                if not skip_meta:
                    payload["meta_eval"] = await meta_eval(payload, meta_sem)
                else:
                    payload["meta_eval"] = {
                        "errors": [], "prompt_edit_proposals": [],
                        "severity": "low", "novel_pattern": False, "_skipped": True,
                    }
                payload["ts"] = datetime.now(timezone.utc).isoformat()
                return payload

            results = await asyncio.gather(*(_process(r) for r in filtered))
            for res in results:
                if res is None:
                    continue
                out_f.write(json.dumps(res, default=str) + "\n")
                written += 1

    logger.info(f"[reclassify] wrote {written} rows → {per_row_path}")
    aggregate(per_row_path, report_path, run_id)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _load_filtered_rows(args) -> list[dict]:
    if args.source == "db":
        rows = load_rows_from_db()
    else:
        rows = json.loads(Path(args.dump).read_text())
    filtered = [r for r in rows if r.get("address") and r.get("origin_key") and has_human_correction(r)]
    if args.limit:
        filtered = filtered[:args.limit]
    logger.info(f"{len(filtered)}/{len(rows)} rows have human corrections (limit={args.limit or 'none'})")
    return filtered


PROMPTS_DIR = _LABELING_DIR / "prompts"
VERSIONS_FILE = PROMPTS_DIR / "versions.json"


def _load_versions() -> dict:
    PROMPTS_DIR.mkdir(exist_ok=True)
    if VERSIONS_FILE.exists():
        return json.loads(VERSIONS_FILE.read_text())
    return {"current": None, "history": []}


def _save_versions(v: dict) -> None:
    VERSIONS_FILE.write_text(json.dumps(v, indent=2))


def _next_version(history: list) -> str:
    if not history:
        return "v1"
    nums = []
    for entry in history:
        m = __import__("re").match(r"v(\d+)$", entry.get("version", ""))
        if m:
            nums.append(int(m.group(1)))
    return f"v{max(nums) + 1}" if nums else "v1"


def _extract_accuracy_summary(run_id: str) -> dict:
    """Pull key metrics from accuracy_report if it exists for this run."""
    report_path = RESULTS_DIR / run_id / "accuracy_report.md"
    if not report_path.exists():
        return {}
    text = report_path.read_text()
    summary = {}
    for line in text.splitlines():
        if line.startswith("| Correct") and "%" in line:
            parts = [p.strip() for p in line.split("|") if p.strip()]
            if len(parts) >= 3:
                summary["category_new"] = parts[2]
        if line.startswith("| Exact") and "%" in line:
            parts = [p.strip() for p in line.split("|") if p.strip()]
            if len(parts) >= 3:
                summary["name_exact_new"] = parts[2]
        if "Wins" in line and "Regressions" in line:
            summary["wins_regressions"] = line.strip().lstrip("**").rstrip("**")
    return summary


def deploy_candidate_prompt(candidate_path: Path, run_id: str) -> None:
    """Write candidate_prompt.md into _SYSTEM_INSTRUCTION in ai_classifier.py.

    Also versions the prompt in backend/labeling/prompts/:
      - saves it as vN.md
      - updates versions.json (current pointer + history)
      - writes current.md as a copy of the active prompt
    """
    import re as _re

    classifier_path = _LABELING_DIR / "ai_classifier.py"
    if not classifier_path.exists():
        raise FileNotFoundError(f"ai_classifier.py not found at {classifier_path}")

    new_prompt = candidate_path.read_text().strip()
    source = classifier_path.read_text()

    pattern = r'(_SYSTEM_INSTRUCTION\s*=\s*""")(.*?)(""")'
    match = _re.search(pattern, source, _re.DOTALL)
    if not match:
        raise ValueError(
            "_SYSTEM_INSTRUCTION triple-quoted string not found in ai_classifier.py. "
            "Check the assignment format."
        )

    old_prompt = match.group(2).strip()
    if old_prompt == new_prompt:
        logger.info("[deploy] _SYSTEM_INSTRUCTION is already up to date — nothing to do.")
        return

    # ── Version the new prompt ────────────────────────────────────────────────
    PROMPTS_DIR.mkdir(exist_ok=True)
    versions = _load_versions()
    version = _next_version(versions["history"])
    versioned_path = PROMPTS_DIR / f"{version}.md"
    versioned_path.write_text(new_prompt)

    # current.md always reflects what's live in ai_classifier.py
    (PROMPTS_DIR / "current.md").write_text(new_prompt)

    accuracy = _extract_accuracy_summary(run_id)
    entry = {
        "version": version,
        "run_id": run_id,
        "deployed_at": datetime.now(timezone.utc).isoformat(),
        "lines": len(new_prompt.splitlines()),
        "accuracy": accuracy,
    }
    versions["current"] = version
    versions["history"].append(entry)
    _save_versions(versions)
    logger.info(f"[deploy] versioned prompt → {versioned_path} (current={version})")

    # ── Archive the outgoing prompt as previous version entry ─────────────────
    # Find its version in history (it's the entry before the one we just added)
    prev_entries = [e for e in versions["history"][:-1]]
    prev_version = prev_entries[-1]["version"] if prev_entries else "unknown"

    # ── Patch ai_classifier.py ────────────────────────────────────────────────
    updated = source[: match.start(2)] + "\n" + new_prompt + "\n" + source[match.end(2):]
    classifier_path.write_text(updated)

    added = len(new_prompt.splitlines()) - len(old_prompt.splitlines())
    sign = "+" if added >= 0 else ""
    logger.info(
        f"[deploy] {prev_version} → {version}: updated _SYSTEM_INSTRUCTION "
        f"({sign}{added} lines, run_id={run_id})"
    )
    if accuracy:
        logger.info(f"[deploy] accuracy: {accuracy}")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--mode",
        choices=["critique", "synthesize", "verify", "full", "deploy", "reclassify"],
        default="critique",
        help=(
            "critique   = Phase 1: DB-only meta-critique from stored features (no enrichment API)\n"
            "synthesize = Phase 2: aggregate proposals → candidate improved prompt\n"
            "verify     = Phase 3: re-enrich + re-classify with candidate prompt, score accuracy\n"
            "full       = all three phases sequentially\n"
            "deploy     = write candidate_prompt.md into _SYSTEM_INSTRUCTION in ai_classifier.py\n"
            "reclassify = legacy: re-enrich → re-classify (current prompt) → meta-eval"
        ),
    )
    ap.add_argument("--source", choices=["json", "db"], default="db",
                    help="json=read from --dump; db=join contract_label_features + contract_label_review")
    ap.add_argument("--dump",
                    help="Path to JSON dump file (--source json only)")
    ap.add_argument("--run-id", default=time.strftime("%Y-%m-%d_%H%M%S"))
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--skip-meta", action="store_true")
    ap.add_argument("--refresh", action="store_true",
                    help="Force re-fetch enrichment cache (verify/reclassify modes only)")
    ap.add_argument("--classify-concurrency", type=int, default=3)
    ap.add_argument("--meta-concurrency", type=int, default=3)
    ap.add_argument("--candidate-prompt",
                    help="Path to candidate prompt file (verify mode; defaults to <run-id>/candidate_prompt.md)")
    args = ap.parse_args()

    run_id = args.run_id

    if args.mode in ("critique", "verify", "full", "reclassify"):
        filtered = _load_filtered_rows(args)

    if args.mode in ("critique", "full"):
        asyncio.run(run_critique_phase(
            filtered, run_id,
            meta_concurrency=args.meta_concurrency,
            skip_meta=args.skip_meta,
        ))

    if args.mode in ("synthesize", "full"):
        from prompt_synthesizer import synthesize_from_run
        asyncio.run(synthesize_from_run(run_id))

    if args.mode in ("verify", "full"):
        candidate_path = (
            Path(args.candidate_prompt) if args.candidate_prompt
            else RESULTS_DIR / run_id / "candidate_prompt.md"
        )
        if not candidate_path.exists():
            raise FileNotFoundError(
                f"Candidate prompt not found: {candidate_path}. Run --mode synthesize first."
            )
        asyncio.run(run_verify_phase(
            filtered, run_id, candidate_path,
            classify_concurrency=args.classify_concurrency,
            meta_concurrency=args.meta_concurrency,
            refresh=args.refresh,
        ))

    if args.mode == "deploy":
        candidate_path = (
            Path(args.candidate_prompt) if args.candidate_prompt
            else RESULTS_DIR / run_id / "candidate_prompt.md"
        )
        if not candidate_path.exists():
            raise FileNotFoundError(
                f"Candidate prompt not found: {candidate_path}. Run --mode synthesize first."
            )
        deploy_candidate_prompt(candidate_path, run_id)

    if args.mode == "reclassify":
        asyncio.run(run_reclassify(
            dump_path=Path(args.dump),
            run_id=run_id,
            limit=args.limit,
            skip_meta=args.skip_meta,
            refresh=args.refresh,
            classify_concurrency=args.classify_concurrency,
            meta_concurrency=args.meta_concurrency,
            source=args.source,
        ))


if __name__ == "__main__":
    main()
