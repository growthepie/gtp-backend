"""Classifier self-critique eval loop.

Pipeline:
  1. Load validation dump → filter to rows with human corrections.
  2. For each row: build/load on-chain context (cache), re-run classify_contract.
  3. Send (ai_rerun, ground_truth, raw_signals, current_system_prompt) to gemini-2.5-pro
     using META_SYSTEM_INSTRUCTION → structured JSON critique + prompt-edit proposals.
  4. Persist per_row.jsonl + aggregated report.md.

CLI:
  python -m backend.labeling.eval.eval_human_corrections \
      --dump backend/local/validation_dump_2026-04-25/all_109_combined.json \
      --run-id 2026-04-27_pass1 [--limit N] [--skip-meta] [--refresh]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import ssl
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
from dotenv import load_dotenv

# ── Path setup (mirror context_cache) ────────────────────────────────────────
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

# Sibling-module imports (this script can be run via `python eval_human_corrections.py`
# from inside the eval/ dir, or `python -m` from anywhere). Add eval/ to sys.path.
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))
import context_cache as cc  # noqa: E402
import re_eval_prompt as rep  # noqa: E402

logger = logging.getLogger("eval.orchestrator")

META_MODEL = "gemini-2.5-pro"

RESULTS_DIR = _BACKEND_DIR / "local" / "eval_results"


# ── Filtering ────────────────────────────────────────────────────────────────

def load_rows_from_db() -> list[dict]:
    """Load ground-truth rows by joining contract_label_review (humans) and contract_label_features (AI baseline).

    Output schema matches the JSON-dump shape consumed by the rest of the orchestrator.
    """
    from src.db_connector import DbConnector
    from sqlalchemy import text as sa_text
    sql = sa_text("""
        SELECT '0x' || encode(f.address, 'hex')           AS address,
               f.origin_key                               AS origin_key,
               f.ai_contract_name                         AS ai_contract_name,
               f.ai_usage_category                        AS ai_usage_category,
               f.ai_confidence                            AS confidence,
               f.ai_reasoning                             AS _comment,
               f.ai_owner_project_likely                  AS is_protocol_contract_likely,
               f.ai_model                                 AS _source,
               r.gtp_contract_name                        AS human_contract_name,
               r.gtp_usage_category                       AS human_usage_category,
               r.gtp_owner_project                        AS owner_project_linked,
               (r.gtp_owner_project IS NOT NULL)          AS gtp_owner_project_confirmed,
               COALESCE(r.gtp_no_owner_project, false)    AS gtp_no_owner_project,
               ''::text                                   AS temp_owner_project,
               ''::text                                   AS human_comment,
               (r.id IS NOT NULL)                         AS approve
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


def has_human_correction(row: dict) -> bool:
    """A row counts as ground-truth iff a human supplied at least one signal."""
    if (row.get("human_contract_name") or "").strip():
        return True
    if (row.get("human_usage_category") or "").strip():
        return True
    if row.get("gtp_owner_project_confirmed") and (row.get("owner_project_linked") or "").strip():
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


# ── Re-classify ──────────────────────────────────────────────────────────────

async def reclassify_row(
    row: dict,
    session: aiohttp.ClientSession,
    enrich_sem: asyncio.Semaphore,
    github_sem: asyncio.Semaphore,
    classify_sem: asyncio.Semaphore,
    refresh: bool = False,
) -> tuple[dict, dict]:
    """Returns (ai_rerun, ctx). ai_rerun is the classifier's full return dict."""
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
    return result, ctx


# ── Meta-eval ────────────────────────────────────────────────────────────────

def _raw_signals_summary(ctx: dict) -> dict:
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
    transfer_summary = [
        {"name": tt.get("token_name"), "symbol": tt.get("token_symbol")}
        for tt in transfers[:10]
    ]
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
        "token_transfers_sample": transfer_summary,
    }


def _meta_user_message(payload: dict) -> str:
    """Pack everything the meta-evaluator needs into a single string user message."""
    return (
        "CURRENT_SYSTEM_PROMPT:\n"
        "<<<\n" + _SYSTEM_INSTRUCTION + "\n>>>\n\n"
        "GROUND_TRUTH:\n" + json.dumps(payload["ground_truth"], indent=2) + "\n\n"
        "AI_RERUN:\n" + json.dumps(payload["ai_rerun"], indent=2, default=str) + "\n\n"
        "AI_ORIGINAL:\n" + json.dumps(payload["ai_original"], indent=2, default=str) + "\n\n"
        "RAW_SIGNALS:\n" + json.dumps(payload["raw_signals"], indent=2, default=str)
    )


async def meta_eval(payload: dict, sem: asyncio.Semaphore) -> dict:
    client = _get_gemini_client()
    user_msg = _meta_user_message(payload)

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

    # Severity histogram
    sev_counts = Counter(r.get("meta_eval", {}).get("severity", "low") for r in rows)
    novel_rows = [r for r in rows if r.get("meta_eval", {}).get("novel_pattern")]
    rows_with_errors = [r for r in rows if r.get("meta_eval", {}).get("errors")]

    # Group errors by (category, root_cause_in_prompt)
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

    # Group prompt edits by target_section
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
    lines.append(f"- Rows with at least one error: **{len(rows_with_errors)}** ({len(rows_with_errors) / n:.0%} of evaluated)" if n else "- Rows with at least one error: 0")
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
        # Show top-3 distinct proposed_change strings by confidence
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


# ── Orchestration ────────────────────────────────────────────────────────────

async def run(
    dump_path: Path,
    run_id: str,
    limit: int = 0,
    skip_meta: bool = False,
    refresh: bool = False,
    classify_concurrency: int = 3,
    meta_concurrency: int = 3,
    source: str = 'json',
) -> None:
    if source == 'db':
        rows = load_rows_from_db()
    else:
        rows = json.loads(dump_path.read_text())
    filtered = [r for r in rows if r.get("address") and r.get("origin_key") and has_human_correction(r)]
    if limit:
        filtered = filtered[:limit]
    logger.info(f"{len(filtered)}/{len(rows)} rows have human corrections (limit={limit or 'none'})")

    out_dir = RESULTS_DIR / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    per_row_path = out_dir / "per_row.jsonl"
    report_path = out_dir / "report.md"

    # Lazy chain config (DB)
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

    logger.info(f"wrote {written} rows → {per_row_path}")
    aggregate(per_row_path, report_path, run_id)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["json", "db"], default="json",
                    help="json=read from --dump file; db=join contract_label_features + contract_label_review")
    ap.add_argument("--dump", default=str(_BACKEND_DIR / "local/validation_dump_2026-04-25/all_109_combined.json"))
    ap.add_argument("--run-id", default=time.strftime("%Y-%m-%d_%H%M%S"))
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--skip-meta", action="store_true")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--classify-concurrency", type=int, default=3)
    ap.add_argument("--meta-concurrency", type=int, default=3)
    args = ap.parse_args()

    asyncio.run(run(
        dump_path=Path(args.dump),
        run_id=args.run_id,
        limit=args.limit,
        skip_meta=args.skip_meta,
        refresh=args.refresh,
        classify_concurrency=args.classify_concurrency,
        meta_concurrency=args.meta_concurrency,
        source=args.source,
    ))


if __name__ == "__main__":
    main()
