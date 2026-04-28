"""On-chain context cache for the classifier eval loop.

For each (address, origin_key) pair this module produces — and persists — the dict
shape that `ai_classifier.classify_contract()` consumes:

    {metrics, blockscout, github, traces, address_logs, token_transfers}

Cache layout: backend/local/eval_cache/<origin_key>__<address_lower>.json

Default: cache forever. Force re-fetch with --refresh on the bulk warmer or
build_or_load(refresh=True) at call sites.
"""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
import sys
from pathlib import Path
from typing import Optional

import aiohttp
from dotenv import load_dotenv

# ── Path setup mirrors automated_labeler.py ──────────────────────────────────
_THIS_DIR = Path(__file__).resolve().parent              # …/backend/labeling/eval
_LABELING_DIR = _THIS_DIR.parent                          # …/backend/labeling
_BACKEND_DIR = _LABELING_DIR.parent                       # …/backend
_REPO_ROOT = _BACKEND_DIR.parent                          # …/gtp-backend

for _p in [str(_LABELING_DIR), str(_BACKEND_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

load_dotenv(dotenv_path=_BACKEND_DIR / ".env", override=False)
load_dotenv(dotenv_path=_REPO_ROOT / ".env", override=False)
load_dotenv(override=False)

from concurrent_contract_analyzer import (  # noqa: E402
    BlockscoutAPI_Async,
    GitHubAPI_Async,
    TenderlyAPI_Async,
    Config,
)
from automated_labeler import (  # noqa: E402
    enrich_contract,
    inject_chain_median_daa,
    fetch_chain_median_daa,
    _load_chain_config,
    ORIGIN_KEY_TO_CHAIN_ID,
)

logger = logging.getLogger("eval.cache")

CACHE_DIR = _BACKEND_DIR / "local" / "eval_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_path(address: str, origin_key: str) -> Path:
    return CACHE_DIR / f"{origin_key}__{address.lower()}.json"


def load_cached(address: str, origin_key: str) -> Optional[dict]:
    p = _cache_path(address, origin_key)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception as e:
        logger.warning(f"cache read failed for {p}: {e}")
        return None


def save_cached(address: str, origin_key: str, ctx: dict) -> None:
    p = _cache_path(address, origin_key)
    p.write_text(json.dumps(ctx, indent=2, default=str))


def _ssl_ctx():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _fetch_db_metrics(address: str, origin_key: str, days: int = 7) -> Optional[dict]:
    """Pull this contract's metrics row from blockspace_fact_contract_level (last N days)."""
    try:
        from src.db_connector import DbConnector
        db = DbConnector()
        sql = f"""
            SELECT
                SUM(txcount)              AS txcount,
                SUM(gas_fees_eth)         AS gas_eth,
                AVG(daa)                  AS avg_daa,
                AVG(success_rate)         AS success_rate
            FROM public.blockspace_fact_contract_level
            WHERE address = decode(replace(lower(:addr), '0x', ''), 'hex')
              AND origin_key = :origin_key
              AND date >= NOW() - INTERVAL '{days} days'
        """
        from sqlalchemy import text as sa_text
        with db.engine.begin() as conn:
            row = conn.execute(sa_text(sql), {"addr": address, "origin_key": origin_key}).fetchone()
        if not row or not row[0]:
            return None
        txcount = int(row[0])
        gas_eth = float(row[1] or 0)
        return {
            "txcount": txcount,
            "gas_eth": gas_eth,
            "avg_daa": float(row[2] or 0),
            "rel_cost": 0.0,  # not load-bearing for re-eval; classifier uses it as a heuristic
            "avg_gas_per_tx": round(gas_eth / txcount, 8) if txcount else 0,
            "day_range": days,
            "success_rate": float(row[3]) if row[3] is not None else None,
        }
    except Exception as e:
        logger.warning(f"DB metrics fetch failed for {address}/{origin_key}: {e}")
        return None


def _seed_metrics_from_dump_row(row: dict, days: int = 7) -> dict:
    """Fallback: build a plausible metrics dict from the validation-dump row when DB read fails."""
    txcount = int(row.get("txcount") or 0)
    gas_eth = 0.0  # not in dump
    avg_daa = float(row.get("avg_daa") or 0)
    rel_cost = float(row.get("rel_cost") or 0)
    sr = row.get("success_rate")
    try:
        sr = float(sr) if sr not in (None, "") else None
    except (TypeError, ValueError):
        sr = None
    return {
        "txcount": txcount,
        "gas_eth": gas_eth,
        "avg_daa": avg_daa,
        "rel_cost": rel_cost,
        "avg_gas_per_tx": 0.0,
        "day_range": days,
        "success_rate": sr,
    }


async def _enrich_one(
    contract: dict,
    session: aiohttp.ClientSession,
    enrich_sem: asyncio.Semaphore,
    github_sem: asyncio.Semaphore,
) -> dict:
    blockscout = BlockscoutAPI_Async(
        session=session,
        api_keys=Config.BLOCKSCOUT_API_KEYS,
        chain_config=Config.CHAIN_EXPLORER_CONFIG,
    )
    github = GitHubAPI_Async(
        session=session,
        headers=Config.get_github_headers(),
        excluded_repos=Config.EXCLUDED_REPOS,
    )
    tenderly = TenderlyAPI_Async(session=session)
    return await enrich_contract(contract, blockscout, github, tenderly, enrich_sem, github_sem)


async def build_or_load(
    address: str,
    origin_key: str,
    *,
    dump_row: Optional[dict] = None,
    refresh: bool = False,
    session: Optional[aiohttp.ClientSession] = None,
    enrich_sem: Optional[asyncio.Semaphore] = None,
    github_sem: Optional[asyncio.Semaphore] = None,
) -> dict:
    """Return the classifier-input dict. Hits cache unless refresh=True."""
    if not refresh:
        cached = load_cached(address, origin_key)
        if cached is not None:
            return cached

    # Cache miss — must run enrichment.
    metrics = _fetch_db_metrics(address, origin_key) or (
        _seed_metrics_from_dump_row(dump_row) if dump_row else {}
    )
    contract = {"address": address, "origin_key": origin_key, "metrics": metrics}

    # chain_median_daa is required by classifier path G threshold.
    inject_chain_median_daa([contract], days=7)

    own_session = session is None
    if own_session:
        connector = aiohttp.TCPConnector(ssl=_ssl_ctx(), limit=10)
        session = aiohttp.ClientSession(connector=connector)
    if enrich_sem is None:
        enrich_sem = asyncio.Semaphore(3)
    if github_sem is None:
        github_sem = asyncio.Semaphore(2)

    try:
        await _enrich_one(contract, session, enrich_sem, github_sem)
    finally:
        if own_session:
            await session.close()

    ctx = {
        "metrics": contract.get("metrics", {}),
        "blockscout": contract.get("blockscout", {}),
        "github": contract.get("github", {"has_valid_repo": False}),
        "traces": contract.get("traces", []),
        "address_logs": contract.get("address_logs", []),
        "token_transfers": contract.get("token_transfers", []),
    }
    save_cached(address, origin_key, ctx)
    return ctx


async def warm_many(rows: list[dict], refresh: bool = False, concurrency: int = 3) -> None:
    """Bulk-build the cache for a list of dump rows.

    Each row needs at minimum: address, origin_key (resolved slug). Skips rows already
    cached unless refresh=True.
    """
    # Lazy chain config load (needs DB engine)
    try:
        from src.db_connector import DbConnector
        _load_chain_config(DbConnector().engine)
    except Exception as e:
        logger.warning(f"chain config load skipped: {e}")

    todo = [
        r for r in rows
        if r.get("address") and r.get("origin_key")
        and (refresh or load_cached(r["address"], r["origin_key"]) is None)
    ]
    logger.info(f"warm: {len(todo)}/{len(rows)} rows need fetching (refresh={refresh})")
    if not todo:
        return

    connector = aiohttp.TCPConnector(ssl=_ssl_ctx(), limit=20)
    enrich_sem = asyncio.Semaphore(concurrency)
    github_sem = asyncio.Semaphore(2)

    async with aiohttp.ClientSession(connector=connector) as session:
        async def _one(r):
            try:
                await build_or_load(
                    r["address"], r["origin_key"],
                    dump_row=r, refresh=refresh,
                    session=session, enrich_sem=enrich_sem, github_sem=github_sem,
                )
                logger.info(f"cached {r['origin_key']}/{r['address']}")
            except Exception as e:
                logger.error(f"warm failed for {r.get('address')}: {e}")

        await asyncio.gather(*(_one(r) for r in todo))


# ── CLI ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", default=str(_BACKEND_DIR / "local/validation_dump_2026-04-25/all_109_combined.json"))
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--concurrency", type=int, default=3)
    args = ap.parse_args()

    rows = json.loads(Path(args.dump).read_text())
    if args.limit:
        rows = rows[: args.limit]
    asyncio.run(warm_many(rows, refresh=args.refresh, concurrency=args.concurrency))
