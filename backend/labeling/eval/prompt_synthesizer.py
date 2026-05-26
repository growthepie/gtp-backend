"""Phase 2: aggregate per-row prompt-edit proposals → candidate improved _SYSTEM_INSTRUCTION.

Reads per_row.jsonl from a critique run, ranks proposals by (count × avg_confidence),
passes them to gemini-2.5-pro with SYNTHESIS_SYSTEM_INSTRUCTION, and writes:
  - candidate_prompt.md       — the complete revised _SYSTEM_INSTRUCTION
  - candidate_prompt_changelog.md — what changed and why

CLI (via eval_human_corrections.py --mode synthesize):
  python -m backend.labeling.eval.eval_human_corrections --mode synthesize --run-id <run-id>

Or directly:
  python backend/labeling/eval/prompt_synthesizer.py --run-id <run-id>
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import sys
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv

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

if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

from ai_classifier import _SYSTEM_INSTRUCTION, _get_gemini_client  # noqa: E402
from google.genai import types as genai_types  # noqa: E402
import re_eval_prompt as rep  # noqa: E402

logger = logging.getLogger("eval.synthesizer")

SYNTHESIS_MODEL = "gemini-2.5-pro"
RESULTS_DIR = _BACKEND_DIR / "local" / "eval_results"


def _aggregate_proposals(per_row_path: Path) -> list[dict]:
    """Read per_row.jsonl → rank proposals by (count × avg_confidence) descending."""
    buckets: dict[str, list[dict]] = defaultdict(list)
    for line in per_row_path.read_text().splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        for p in r.get("meta_eval", {}).get("prompt_edit_proposals", []):
            section = (p.get("target_section") or "?").strip()
            buckets[section].append({
                "proposed_change": p.get("proposed_change", ""),
                "rationale": p.get("rationale", ""),
                "confidence": float(p.get("confidence", 0)),
            })

    ranked: list[dict] = []
    for section, props in buckets.items():
        count = len(props)
        avg_conf = sum(p["confidence"] for p in props) / max(count, 1)
        score = count * avg_conf
        # Deduplicate proposals for the same section by proposed_change prefix
        seen: set[str] = set()
        deduped = []
        for p in sorted(props, key=lambda x: -x["confidence"]):
            key = p["proposed_change"][:100]
            if key not in seen:
                seen.add(key)
                deduped.append(p)
        ranked.append({
            "target_section": section,
            "proposals": deduped,
            "count": count,
            "avg_confidence": round(avg_conf, 3),
            "score": round(score, 3),
        })

    ranked.sort(key=lambda x: -x["score"])
    logger.info(f"aggregated {len(ranked)} proposal sections from {per_row_path.name}")
    return ranked


def _build_synthesis_user_message(ranked_proposals: list[dict]) -> str:
    return (
        "CURRENT_PROMPT:\n"
        "<<<\n" + _SYSTEM_INSTRUCTION + "\n>>>\n\n"
        "RANKED_PROPOSALS:\n"
        + json.dumps(ranked_proposals, indent=2)
    )


async def synthesize_from_run(run_id: str) -> Path:
    """Read critique results for run_id, call Pro, write candidate prompt. Returns prompt path."""
    out_dir = RESULTS_DIR / run_id
    per_row_path = out_dir / "per_row.jsonl"
    candidate_path = out_dir / "candidate_prompt.md"
    changelog_path = out_dir / "candidate_prompt_changelog.md"

    if not per_row_path.exists():
        raise FileNotFoundError(
            f"per_row.jsonl not found at {per_row_path}. Run --mode critique first."
        )

    ranked = _aggregate_proposals(per_row_path)
    if not ranked:
        logger.warning("no proposals found in per_row.jsonl — writing unchanged prompt")
        candidate_path.write_text(_SYSTEM_INSTRUCTION)
        changelog_path.write_text("# Changelog\n\nNo proposals — prompt unchanged.\n")
        return candidate_path

    user_msg = _build_synthesis_user_message(ranked)
    client = _get_gemini_client()

    def _call():
        return client.models.generate_content(
            model=SYNTHESIS_MODEL,
            contents=user_msg,
            config=genai_types.GenerateContentConfig(
                system_instruction=rep.SYNTHESIS_SYSTEM_INSTRUCTION,
                response_mime_type="application/json",
                response_schema=rep.SYNTHESIS_RESPONSE_SCHEMA,
                temperature=0.2,
                max_output_tokens=32768,
            ),
        )

    logger.info(f"calling {SYNTHESIS_MODEL} for prompt synthesis ({len(ranked)} proposal sections)...")
    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(None, _call)
    raw = response.text or ""

    cleaned = re.sub(r",\s*([}\]])", r"\1", raw)
    try:
        result = json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.error(f"synthesis JSON parse failed: {e} | raw[:300]: {raw[:300]!r}")
        # Fallback: write unchanged prompt so downstream steps don't hard-fail
        candidate_path.write_text(_SYSTEM_INSTRUCTION)
        changelog_path.write_text(f"# Changelog\n\nParse error — prompt unchanged.\n\n```\n{raw[:500]}\n```\n")
        return candidate_path

    revised = result.get("revised_prompt") or _SYSTEM_INSTRUCTION
    candidate_path.write_text(revised)
    logger.info(f"wrote candidate prompt → {candidate_path} ({len(revised)} chars)")

    skip_reason = result.get("skip_reason") or ""
    changelog = result.get("changelog") or []
    unchanged = result.get("sections_unchanged") or []

    cl_lines = [f"# Prompt Changelog — run `{run_id}`", ""]
    if skip_reason:
        cl_lines.append(f"**No changes applied:** {skip_reason}")
    else:
        cl_lines.append(f"**Sections changed: {len(changelog)}**  |  "
                        f"Sections unchanged: {len(unchanged)}")
        cl_lines.append("")
        cl_lines.append("## Changes")
        cl_lines.append("")
        for entry in changelog:
            cl_lines.append(f"### `{entry.get('section', '?')}`")
            cl_lines.append(entry.get("change_summary", ""))
            cl_lines.append("")
        if unchanged:
            cl_lines.append("## Unchanged sections")
            cl_lines.append("")
            for s in unchanged:
                cl_lines.append(f"- `{s}`")
    cl_lines.append("")
    cl_lines.append("## Ranked proposals fed to synthesizer")
    cl_lines.append("")
    for p in ranked:
        cl_lines.append(
            f"- `{p['target_section']}` — count={p['count']} "
            f"avg_conf={p['avg_confidence']} score={p['score']}"
        )

    changelog_path.write_text("\n".join(cl_lines) + "\n")
    logger.info(f"wrote changelog → {changelog_path}")
    return candidate_path


if __name__ == "__main__":
    import argparse
    import time

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True, help="Run ID from a completed critique phase")
    args = ap.parse_args()
    asyncio.run(synthesize_from_run(args.run_id))
