"""
Automated Contract Labeler
==========================
Replaces the Airtable-based pipeline. Fetches unlabeled contracts directly from
the production DB, enriches each contract via Blockscout + GitHub + Tenderly,
classifies via Claude, and outputs an OLI-format JSON/CSV ready for off-chain
attestation.

USAGE
-----
Run from the backend repo root (where src/ is importable):

    python path/to/git_search/automated_labeler.py \\
        --chains base arbitrum \\
        --days 7 \\
        --max-contracts 50 \\
        --concurrency 5 \\
        --output-dir ./output

REQUIREMENTS
------------
- pip install oli-python anthropic pyyaml aiohttp python-dotenv pandas
- ANTHROPIC_API_KEY, TENDERLY_ACCESS_KEY in .env
- Blockscout API keys for target chains (same as concurrent_contract_analyzer.py)
"""

import asyncio
import aiohttp
import argparse
import csv
import json
import logging
import os
import random
import ssl
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# ── Resolve paths ─────────────────────────────────────────────────────────────
# Layout: gtp-backend/backend/labeling/  (this file)
#         gtp-backend/backend/src/       (db_connector, main_config)
# We add both this dir (for sibling labeling scripts) and the parent (backend/)
# so that `from src.db_connector import DbConnector` works without any setup.py.
_THIS_DIR = Path(__file__).resolve().parent      # …/backend/labeling
_BACKEND_DIR = _THIS_DIR.parent                  # …/backend

for _p in [str(_THIS_DIR), str(_BACKEND_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

load_dotenv(dotenv_path=_BACKEND_DIR / ".env", override=False)
load_dotenv(dotenv_path=_BACKEND_DIR.parent / ".env", override=False)  # repo root .env
load_dotenv(override=False)  # fallback to cwd .env

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("AutoLabeler")

# ── Imports from existing scripts ────────────────────────────────────────────
try:
    from concurrent_contract_analyzer import (
        BlockscoutAPI_Async,
        GitHubAPI_Async,
        TenderlyAPI_Async,
        Config,
        create_ssl_context,
        rate_limit_manager,
    )
    from ai_classifier import classify_contract, CHAIN_TO_EIP155, _oli_cache, decode_log_signals
except ImportError as e:
    logger.error(f"Import error — ensure git_search/ is on sys.path: {e}")
    raise

# ── DB import (requires being run from the backend repo) ─────────────────────
try:
    from src.db_connector import DbConnector
    from src.main_config import get_main_config
    _HAS_DB = True
except ImportError:
    logger.warning(
        "Could not import src.db_connector — run from the backend repo root, "
        "or use --input-json to provide a pre-exported contract list."
    )
    _HAS_DB = False

# ── oli-python ────────────────────────────────────────────────────────────────
try:
    import oli
    _HAS_OLI = True
except ImportError:
    logger.warning("oli-python not installed — skipping OLI validation. Run: pip install oli-python")
    _HAS_OLI = False

# ── EIP-155 chain ID mapping ─────────────────────────────────────────────────
# Populated by _load_chain_config() after DB init — do not add hardcoded entries.
ORIGIN_KEY_TO_CHAIN_ID: dict[str, int] = {}


def _load_chain_config(engine) -> None:
    """Load chain mappings from sys_main_conf into Config and ai_classifier."""
    import ai_classifier
    Config.load_from_db(engine)
    ai_classifier.CHAIN_TO_EIP155.update({
        ok: caip2
        for ok, chain_id in Config.SUPPORTED_CHAINS.items()
        for caip2 in [f"eip155:{chain_id}"]
    })
    ORIGIN_KEY_TO_CHAIN_ID.update(Config.SUPPORTED_CHAINS)


def _ssl_ctx():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


# ─── DB fetch ────────────────────────────────────────────────────────────────

def fetch_unlabeled_contracts(
    days: int,
    origin_keys: Optional[list[str]],
    max_contracts: int,
    min_txcount: int = 0,   # kept for CLI backwards compat; ignored when use_v2=True
    use_v2: bool = True,    # use get_unlabelled_contracts_v2 (0.1% + top-10 per chain)
    per_chain_limit: int = 0,  # 0 = no per-chain cap
) -> list[dict]:
    """Pull unlabeled contracts from the production DB and return as a list of dicts.

    When use_v2=True (default), uses get_unlabelled_contracts_v2 which applies a 0.1%
    relative txcount threshold and returns at most top-10 per chain from passing contracts.
    The absolute min_txcount arg is ignored in v2 mode.
    """
    if not _HAS_DB:
        raise RuntimeError(
            "src.db_connector is not importable. "
            "Run this script from the backend repo root or use --input-json."
        )

    db = DbConnector()
    _load_chain_config(db.engine)
    main_conf = get_main_config()

    if not origin_keys:
        origin_keys = [
            c.origin_key for c in main_conf
            if c.api_in_main
            and 'blockspace' not in c.api_exclude_metrics
            and c.origin_key in ORIGIN_KEY_TO_CHAIN_ID
        ]
        logger.info(f"Auto-detected {len(origin_keys)} chains with full enrichment: {origin_keys}")

    if use_v2:
        logger.info(f"Fetching unlabeled contracts via v2 (0.1% threshold + top-10) for days={days}, chains={origin_keys}")
        df = db.get_unlabelled_contracts_v2(days, origin_keys)
        # v2 returns 'txcount' directly (summed); normalise column name for consistency
        if 'gas_eth' not in df.columns and 'gas_fees_eth' in df.columns:
            df = df.rename(columns={'gas_fees_eth': 'gas_eth'})
    else:
        logger.info(f"Fetching unlabeled contracts via v1 for days={days}, chains={origin_keys}")
        df = db.get_contracts_category_comparison('unlabeled', days, origin_keys)

    # Normalise address bytes → hex string
    df['address'] = df['address'].apply(lambda x: '0x' + bytes(x).hex() if isinstance(x, (bytes, bytearray, memoryview)) else x)

    if not use_v2:
        # v1 path: apply absolute floor and sort
        df = df[df['txcount'] >= min_txcount]
        df = df.sort_values('txcount', ascending=False)

    df = df.drop_duplicates(subset=['address', 'origin_key'])

    if per_chain_limit > 0:
        df = df.groupby('origin_key', group_keys=False).head(per_chain_limit)

    # Secondary dedup against Airtable "Label Pool Automated" — guards the window between
    # attestation submission and OLI indexer writing the label back to public.labels.
    # The DB exclusion handles steady-state (next-day runs); this handles same-day re-runs.
    # We match on address only because origin_key in Airtable is a linked record (record ID),
    # not a plain text chain key. The DB exclusion already handles per-chain precision.
    try:
        import os
        import src.misc.airtable_functions as at
        from pyairtable import Api
        _api = Api(os.getenv('AIRTABLE_API_KEY'))
        _tbl = _api.table(os.getenv('AIRTABLE_BASE_ID'), 'Label Pool Automated')
        df_at = at.read_airtable(_tbl)
        if not df_at.empty and 'address' in df_at.columns:
            before = len(df)
            # Normalise to lowercase for comparison
            already_queued = set(df_at['address'].str.lower())
            df = df[~df['address'].str.lower().isin(already_queued)]
            logger.info(f"Airtable dedup removed {before - len(df)} already-queued contracts")
    except Exception as e:
        logger.warning(f"Airtable dedup skipped (non-fatal): {e}")

    df = df.head(max_contracts)
    logger.info(f"After filtering: {len(df)} contracts to label")

    records = []
    for _, row in df.iterrows():
        gas_eth = float(row.get('gas_fees_eth') or row.get('gas_eth') or 0)
        txcount = int(row.get('txcount') or 0)
        # DB v2 returns avg_daa (absolute daily active addresses count); fallback to daa for v1
        sr = row.get('avg_success') or row.get('success_rate')
        rc = row.get('rel_cost')
        records.append({
            'address': row['address'],
            'origin_key': row['origin_key'],
            'metrics': {
                'txcount': txcount,
                'gas_eth': gas_eth,
                'avg_daa': float(row.get('avg_daa') or row.get('daa') or 0),
                'rel_cost': float(rc) if rc is not None else 0.0,
                'avg_gas_per_tx': round(gas_eth / txcount, 8) if txcount else 0,
                'day_range': days,
                'success_rate': float(sr) if sr is not None else None,
            },
        })
    return records


def fetch_chain_median_daa(origin_keys: list[str], days: int = 7) -> dict[str, float]:
    """Query the DB for median avg_daa of top contracts (>=0.1% of chain txcount) per chain.

    Returns a dict mapping origin_key -> median_daa. Falls back to empty dict on error.
    """
    try:
        db = DbConnector()
        keys_str = "','".join(origin_keys)
        sql = f"""
            WITH chain_totals AS (
                SELECT origin_key, SUM(txcount) AS total_txcount
                FROM public.blockspace_fact_contract_level
                WHERE date >= NOW() - INTERVAL '{days} days'
                  AND origin_key IN ('{keys_str}')
                GROUP BY origin_key
            ),
            contract_agg AS (
                SELECT
                    cl.origin_key,
                    cl.address,
                    AVG(cl.daa) AS avg_daa,
                    MAX(ct.total_txcount) AS chain_total
                FROM public.blockspace_fact_contract_level cl
                JOIN chain_totals ct ON cl.origin_key = ct.origin_key
                WHERE cl.date >= NOW() - INTERVAL '{days} days'
                  AND cl.origin_key IN ('{keys_str}')
                  AND cl.daa > 0
                GROUP BY cl.origin_key, cl.address
                HAVING SUM(cl.txcount) >= MAX(ct.total_txcount) * 0.001
            )
            SELECT origin_key, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_daa) AS median_daa
            FROM contract_agg
            GROUP BY origin_key
        """
        import pandas as pd
        df = pd.read_sql(sql, db.engine)
        result = dict(zip(df['origin_key'], df['median_daa'].astype(float)))
        logger.info(f"Chain median DAA: { {k: f'{v:.1f}' for k, v in result.items()} }")
        return result
    except Exception as e:
        logger.warning(f"Could not fetch chain median DAA from DB (falling back to 0): {e}")
        return {}


def inject_chain_median_daa(contracts: list[dict], days: int = 7) -> None:
    """Fetch per-chain median avg_daa from the DB and store in each contract's metrics.

    Used by classify_contract to set a dynamic PUBLIC caller threshold:
    a contract with avg_daa far below its chain median is a small operator set,
    not a truly public-facing contract. Skips contracts that already have chain_median_daa set.
    """
    # Only fetch for contracts that don't already have chain_median_daa
    chains_needed = list({
        c['origin_key'] for c in contracts
        if c.get('metrics', {}).get('chain_median_daa', 0.0) == 0.0
    })

    chain_median: dict[str, float] = {}
    if chains_needed:
        chain_median = fetch_chain_median_daa(chains_needed, days=days)

    for c in contracts:
        if c.get('metrics', {}).get('chain_median_daa', 0.0) > 0:
            continue
        median = chain_median.get(c['origin_key'], 0.0)
        c.setdefault('metrics', {})['chain_median_daa'] = median


def load_contracts_from_json(path: str) -> list[dict]:
    """Load a pre-exported contract list (JSON array) as fallback for --input-json.

    Normalizes raw DB-style field names (gas_fees_eth, daa) into the nested
    'metrics' dict that enrich_contract / classify_contract expect.
    """
    with open(path) as f:
        data = json.load(f)
    logger.info(f"Loaded {len(data)} contracts from {path}")

    # Build reverse map eip155:N → origin_key for JSON files that only have chain_id
    _eip_to_origin = {v: k for k, v in CHAIN_TO_EIP155.items()}

    for c in data:
        # Derive origin_key from chain_id if missing (output JSON uses eip155 format)
        if 'origin_key' not in c and 'chain_id' in c:
            c['origin_key'] = _eip_to_origin.get(c['chain_id'], c['chain_id'])

        # Wrap top-level label fields into a 'label' dict if missing (output JSON is flat)
        if 'label' not in c and 'usage_category' in c:
            c['label'] = {
                'usage_category': c.get('usage_category'),
                'contract_name': c.get('contract_name'),
                'confidence': c.get('confidence'),
                'reasoning': c.get('reasoning'),
            }

        if 'metrics' not in c:
            # Output JSON uses gas_eth/avg_daa; DB export uses gas_fees_eth/daa
            gas_eth = float(c.get('gas_eth') or c.get('gas_fees_eth') or 0)
            txcount = int(c.get('txcount', 0) or 0)
            c['metrics'] = {
                'txcount': txcount,
                'gas_eth': gas_eth,
                'avg_daa': float(c.get('avg_daa') or c.get('daa') or 0),
                'rel_cost': float(c.get('rel_cost', 0) or 0),
                'avg_gas_per_tx': round(gas_eth / txcount, 8) if txcount else 0,
                'success_rate': c.get('success_rate'),
                'day_range': int(c.get('day_range', 7) or 7),
            }
    return data


# ─── Enrichment ──────────────────────────────────────────────────────────────

async def enrich_contract(
    contract: dict,
    blockscout: BlockscoutAPI_Async,
    github: GitHubAPI_Async,
    tenderly: TenderlyAPI_Async,
    sem: asyncio.Semaphore,
    github_sem: asyncio.Semaphore,
) -> dict:
    """Fetch Blockscout info, sample tx hashes, GitHub presence, and Tenderly traces."""
    async with sem:
        address = contract['address']
        origin_key = contract['origin_key']
        chain_id = ORIGIN_KEY_TO_CHAIN_ID.get(origin_key)

        if not chain_id:
            logger.warning(f"No chain_id mapping for origin_key={origin_key}, skipping enrichment")
            contract['blockscout'] = {}
            contract['github'] = {'has_valid_repo': False}
            contract['traces'] = []
            return contract

        # 1. Fetch contract info first — needed to gate logs fetch
        async def _get_info():
            try:
                return await blockscout.get_contract_info(address, chain_id)
            except Exception as e:
                logger.debug(f"[Blockscout] get_contract_info failed for {address}: {e}")
                return {}

        bs_info = await _get_info()

        # 2. Fetch txs with one retry on failure (Blockscout can timeout under concurrent load)
        async def _get_txs():
            for attempt in range(2):
                try:
                    result = await blockscout.get_transactions(address, chain_id, limit=50)
                    if result:
                        return result
                except Exception as e:
                    logger.debug(f"[Blockscout] get_transactions attempt {attempt + 1} failed for {address}: {e}")
                if attempt == 0:
                    await asyncio.sleep(1.0)
            return []

        # 4. Skip logs for unverified+unnamed contracts — they don't emit useful events
        #    (raw proxy/bot contracts with no ABI emit nothing we can decode)
        _skip_logs = not bs_info.get('is_verified') and not bs_info.get('contract_name')

        async def _get_logs():
            if _skip_logs:
                return []
            try:
                return await blockscout.get_address_logs(address, chain_id, limit=20)
            except Exception as e:
                logger.debug(f"[Blockscout] get_address_logs failed for {address}: {e}")
                return []

        async def _get_token_transfers():
            try:
                return await blockscout.get_token_transfers(address, chain_id, limit=20)
            except Exception as e:
                logger.debug(f"[Blockscout] get_token_transfers failed for {address}: {e}")
                return []

        txs, address_logs, token_transfers = await asyncio.gather(
            _get_txs(), _get_logs(), _get_token_transfers()
        )

        # Proxy impl lookup: when proxy has no resolved name, fetch the implementation
        # contract directly — costs 20 credits but resolves the identity for PATH B.
        if bs_info.get('is_proxy') and not bs_info.get('contract_name') and bs_info.get('impl_address'):
            try:
                impl_info = await blockscout.get_contract_info(bs_info['impl_address'], chain_id)
                if impl_info.get('contract_name'):
                    bs_info['contract_name'] = impl_info['contract_name']
                    bs_info['impl_verified'] = impl_info.get('is_verified', False)
                    logger.debug(f"[Blockscout] Resolved proxy impl for {address}: {impl_info['contract_name']!r}")
            except Exception as e:
                logger.debug(f"[Blockscout] proxy impl lookup failed for {address}: {e}")

        # Prefer successful txs — Tenderly has better trace data for non-reverted calls.
        # Fall back to all txs if there are no successes (e.g., pure MEV bot with high fail rate).
        ok_hashes = [t['hash'] for t in txs if t.get('hash') and t.get('status') == 'ok']
        all_hashes = ok_hashes if ok_hashes else [t['hash'] for t in txs if t.get('hash')]
        sample_hashes = random.sample(all_hashes, min(10, len(all_hashes)))
        logger.debug(f"Sampled {len(sample_hashes)} tx hashes for {address}")
        logger.debug(f"[Blockscout] Got {len(address_logs)} address logs for {address} (skipped={_skip_logs})")

        # 3. GitHub: check repo presence for all contracts.
        has_repo, repo_count = False, 0
        try:
            async with github_sem:
                has_repo, repo_count = await github.search_contract_address(address)
            logger.debug(f"[GitHub] {address}: has_repo={has_repo} count={repo_count}")
        except Exception as e:
            logger.debug(f"[GitHub] search failed for {address}: {e}")

        # 5. Tenderly: trace sampled txs
        traces = []
        if sample_hashes and chain_id:
            trace_tasks = [
                tenderly.trace_transaction(h, chain_id)
                for h in sample_hashes
            ]
            trace_results = await asyncio.gather(*trace_tasks, return_exceptions=True)
            for r in trace_results:
                if isinstance(r, dict) and r:
                    traces.append(r)

        # 6. Tenderly range fallback: when Blockscout txs exist but all traces are empty/failing,
        #    use tenderly_getTransactionsRange to find direct top-level txs TO this contract.
        #    This handles contracts whose Blockscout tx list is dominated by internal calls.
        _tenderly_key = Config.TENDERLY_ACCESS_KEY
        if not traces and _tenderly_key and chain_id:
            txcount_7d = contract.get('metrics', {}).get('txcount', 0)
            range_hashes = await tenderly.get_transactions_range(
                address, chain_id, _tenderly_key, txcount_7d=txcount_7d
            )
            if range_hashes:
                fallback_tasks = [tenderly.trace_transaction(h, chain_id) for h in range_hashes]
                fallback_results = await asyncio.gather(*fallback_tasks, return_exceptions=True)
                for r in fallback_results:
                    if isinstance(r, dict) and r:
                        traces.append(r)

        contract['blockscout'] = bs_info
        contract['github'] = {'has_valid_repo': has_repo, 'repo_count': repo_count}
        contract['traces'] = traces
        contract['address_logs'] = address_logs
        contract['token_transfers'] = token_transfers
        # avg_gas_per_tx is derived from DB metrics (success_rate already set from DB)
        m = contract.setdefault('metrics', {})
        txcount = m.get('txcount', 0)
        gas_eth = m.get('gas_eth', 0)
        m['avg_gas_per_tx'] = round(gas_eth / txcount, 8) if txcount else 0

        logger.info(
            f"Enriched {address} ({origin_key}): "
            f"name={bs_info.get('contract_name', '')!r} "
            f"verified={bs_info.get('is_verified')} "
            f"github={has_repo} "
            f"traces={len(traces)}"
        )
        return contract


# ─── Output ──────────────────────────────────────────────────────────────────

def _oli_validate_category(category_id: str, valid_ids: list[str]) -> str:
    """Use oli-python to validate category, fallback to 'other' if invalid."""
    if not _HAS_OLI:
        return category_id if category_id in valid_ids else 'other'
    try:
        # oli-python may provide a validate function; gracefully degrade if not
        if hasattr(oli, 'validate_category'):
            return oli.validate_category(category_id) or 'other'
        # Alternatively, oli may expose the schema for lookup
        return category_id if category_id in valid_ids else 'other'
    except Exception:
        return category_id if category_id in valid_ids else 'other'


def build_attestation_rows(labeled_contracts: list[dict], valid_ids: list[str]) -> list[dict]:
    """Convert enriched+labeled contracts to OLI attestation row dicts."""
    rows = []
    for c in labeled_contracts:
        label = c.get('label', {})
        origin_key = c['origin_key']
        chain_id_eip = CHAIN_TO_EIP155.get(origin_key, f"eip155:0")
        usage_cat = _oli_validate_category(label.get('usage_category', 'other'), valid_ids)

        m = c.get('metrics', {})
        rows.append({
            'chain_id': chain_id_eip,
            'address': c['address'],
            'contract_name': label.get('contract_name', ''),
            'owner_project': None,
            'usage_category': usage_cat,
            'confidence': round(label.get('confidence', 0.0), 3),
            'reasoning': label.get('reasoning', ''),
            'source': 'gtp-automated_labeler_v1',
            'github_found': c.get('github', {}).get('has_valid_repo', False),
            # metrics
            'txcount': m.get('txcount', 0),
            'gas_eth': m.get('gas_eth', 0),
            'avg_daa': m.get('avg_daa', 0),
            'rel_cost': m.get('rel_cost', 0),
            'avg_gas_per_tx': m.get('avg_gas_per_tx', 0),
            'success_rate': m.get('success_rate'),
            'day_range': m.get('day_range', 7),
        })
    return rows


def _is_protocol_likely(label: dict, blockscout: dict, log_signals: dict, metrics: dict) -> bool:
    """Return True when on-chain signals suggest this is a public protocol contract.

    Used by build_attestation_rows_v2 to set owner_project = 'protocol-contract-likely'
    so the contract surfaces in 'Label Pool Reattest' for human owner_project assignment.

    is_verified is checked FIRST — MEV bots almost never publish verified source code,
    so a Blockscout-verified contract should get the sentinel regardless of AI category.
    """
    # OVERRIDE: Blockscout verification = near-certain public protocol, regardless of AI category.
    # Checked before hard exclusions so verified contracts mislabeled as 'trading'/'other' still surface.
    if blockscout.get('is_verified'):
        return True

    category = label.get('usage_category', '')
    confidence = label.get('confidence', 0.0)
    success_rate = metrics.get('success_rate')
    # Treat None success_rate as non-bot (don't penalise missing data)
    if success_rate is None:
        success_rate = 1.0

    # Novel protocol tokens in every tx override trading exclusion.
    # A contract that interacts with specific protocol tokens on ≥50% of traces
    # is protocol-specific infrastructure even when classified as trading.
    if label.get('novel_tokens'):
        return True

    # Hard exclusions (only applies when NOT verified)
    if category in ('trading', 'other'):
        return False
    if confidence < 0.50:
        return False
    if success_rate < 0.40:
        return False

    # Fast-pass: categories that are always public protocol contracts.
    # bridge/lending/erc4337 come from pre-computed callee matches (W4 fix).
    # dex/stablecoin/oracle are confirmed protocol-only from human review data.
    if category in ('bridge', 'lending', 'erc4337', 'cc_communication', 'derivative',
                    'dex', 'stablecoin', 'oracle'):
        return True

    # Strong positive signals — one is sufficient
    if log_signals.get('has_governance'):
        return True
    if log_signals.get('has_staking'):
        return True
    if log_signals.get('has_bridge_events'):
        return True
    if log_signals.get('has_erc4337'):
        return True
    if log_signals.get('has_valid_repo'):
        return True

    # Moderate: specific non-trading category with decent confidence
    return confidence >= 0.65


def build_attestation_rows_v2(labeled_contracts: list[dict], valid_ids: list[str]) -> list[dict]:
    """Like build_attestation_rows but with two additions:

    1. Sets owner_project = 'protocol-contract-likely' when _is_protocol_likely() is True.
       This sentinel flows into 'Label Pool Reattest' via a dedicated DAG task so reviewers
       can assign a real owner_project slug.

    2. Sets contract_name = 'Ambiguous Contract' when usage_category = 'other', making the
       intent explicit in Airtable. OLI still receives usage_category = 'other' (valid schema).
    """
    rows = []
    skipped = 0
    for c in labeled_contracts:
        # Skip contracts where enrichment clearly failed — no Blockscout data, no traces,
        # no logs. These fall to PATH G0 → other/AmbiguousContract 0.30 due to empty API
        # returns (not genuine ambiguity). Labeling them would pollute the attestation pool.
        bs = c.get('blockscout', {})
        enrichment_failed = (
            not bs.get('contract_name')
            and not bs.get('is_verified')
            and not c.get('traces')
            and not c.get('address_logs')
        )
        if enrichment_failed:
            logger.warning(
                f"[skip] {c.get('address')} ({c.get('origin_key')}): "
                f"enrichment returned no data — skipping to avoid bad label"
            )
            skipped += 1
            continue

        label = c.get('label', {})
        origin_key = c['origin_key']
        chain_id_eip = CHAIN_TO_EIP155.get(origin_key, "eip155:0")
        usage_cat = _oli_validate_category(label.get('usage_category', 'other'), valid_ids)

        contract_name = label.get('contract_name', '')
        if usage_cat == 'other':
            contract_name = 'Ambiguous Contract'

        log_signals = decode_log_signals(c.get('address_logs', []))
        log_signals['has_valid_repo'] = c.get('github', {}).get('has_valid_repo', False)

        owner_project = None
        if _is_protocol_likely(label, c.get('blockscout', {}), log_signals, c.get('metrics', {})):
            owner_project = 'protocol-contract-likely'

        m = c.get('metrics', {})
        rows.append({
            'chain_id': chain_id_eip,
            'address': c['address'],
            'contract_name': contract_name,
            'owner_project': owner_project,
            'usage_category': usage_cat,
            'confidence': round(label.get('confidence', 0.0), 3),
            'reasoning': label.get('reasoning', ''),
            'source': 'gtp-automated_labeler_v1',
            'github_found': c.get('github', {}).get('has_valid_repo', False) or c.get('github_found', False),
            'is_verified': c.get('blockscout', {}).get('is_verified', False) or c.get('is_verified', False),
            'txcount': m.get('txcount', 0),
            'gas_eth': m.get('gas_eth', 0),
            'avg_daa': m.get('avg_daa', 0),
            'rel_cost': m.get('rel_cost', 0),
            'avg_gas_per_tx': m.get('avg_gas_per_tx', 0),
            'success_rate': m.get('success_rate'),
            'day_range': m.get('day_range', 7),
        })
    if skipped:
        logger.info(f"Skipped {skipped} contracts with empty enrichment data (API fetch failures)")
    return rows


def write_output(rows: list[dict], output_dir: str, prefix: str = "labeled_contracts") -> tuple[str, str]:
    """Write JSON and CSV attestation files. Returns (json_path, csv_path)."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M')
    json_path = str(Path(output_dir) / f"{prefix}_{ts}.json")
    csv_path = str(Path(output_dir) / f"{prefix}_{ts}.csv")

    with open(json_path, 'w') as f:
        json.dump(rows, f, indent=2)
    logger.info(f"Wrote {len(rows)} rows → {json_path}")

    csv_cols = ['chain_id', 'address', 'contract_name', 'owner_project', 'usage_category', 'confidence']
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=csv_cols, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Wrote {len(rows)} rows → {csv_path}")

    return json_path, csv_path


# ─── Airtable write ──────────────────────────────────────────────────────────

def _write_attested_to_airtable(rows: list[dict]) -> None:
    """Push attested rows with full metrics to 'Label Pool Automated' in Airtable.

    Deduplicates against existing rows (address + chain_id) so re-runs are safe.
    Requires AIRTABLE_API_KEY and AIRTABLE_BASE_ID env vars.
    """
    try:
        import pandas as pd
        from pyairtable import Api
        from eth_utils import to_checksum_address

        _backend_dir = Path(__file__).resolve().parent.parent
        for _p in [str(_backend_dir)]:
            if _p not in sys.path:
                sys.path.insert(0, _p)
        import src.misc.airtable_functions as at
    except ImportError as e:
        logger.warning(f"[Airtable] Skipping write — missing dependency: {e}")
        return

    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("AIRTABLE_BASE_ID")
    if not api_key or not base_id:
        logger.warning("[Airtable] AIRTABLE_API_KEY or AIRTABLE_BASE_ID not set — skipping write")
        return

    try:
        api = Api(api_key)
        table = api.table(base_id, 'Label Pool Automated')

        df = pd.DataFrame(rows)

        # chain_id (eip155:X) → caip2 for Chains lookup; address stays as-is
        df = df.rename(columns={'chain_id': 'caip2'})

        # Dedup against existing rows
        df_existing = at.read_airtable(table)
        if not df_existing.empty and 'address' in df_existing.columns and 'caip2' in df_existing.columns:
            df = df.merge(df_existing[['address', 'caip2']], on=['address', 'caip2'],
                          how='left', indicator=True)
            df = df[df['_merge'] == 'left_only'].drop(columns=['_merge'])

        if df.empty:
            logger.info("[Airtable] No new rows to write (all already present).")
            return

        # Map usage_category → Airtable Sub Categories record ID
        cat_table = api.table(base_id, 'Sub Categories')
        df_cat = at.read_airtable(cat_table)
        df = df.replace({'usage_category': df_cat.set_index('category_id')['id']})
        df['usage_category'] = df['usage_category'].apply(
            lambda x: [x] if pd.notna(x) and str(x).startswith('rec') else [])

        # Map caip2 → Airtable Chains record ID, rename to origin_key
        chains_table = api.table(base_id, 'Chains')
        df_chains = at.read_airtable(chains_table)
        df = df.replace({'caip2': df_chains.set_index('caip2')['id']})
        df['caip2'] = df['caip2'].apply(
            lambda x: [x] if pd.notna(x) and str(x).startswith('rec') else [])
        df = df.rename(columns={'caip2': 'origin_key'})

        # Add attester public address (always the automated labeler key)
        df['attester'] = '0xaDbf2b56995b57525Aa9a45df19091a2C5a2A970'

        # Build _comment from confidence + reasoning (mirrors the OLI tag value)
        def _build_comment(row):
            conf = row.get('confidence')
            rsn = row.get('reasoning', '') or ''
            return f"confidence: {conf} | {rsn}" if rsn else (f"confidence: {conf}" if conf is not None else None)
        df['_comment'] = df.apply(_build_comment, axis=1)

        # Rename source → _source to match Airtable column name and OLI tag ID
        if 'source' in df.columns:
            df = df.rename(columns={'source': '_source'})

        # Flag sentinel rows before dropping owner_project
        df['is_protocol_contract_likely'] = df.get('owner_project', pd.Series(dtype=str)) == 'protocol-contract-likely'

        # Keep only columns that exist in Airtable — drop everything else
        AIRTABLE_COLS = {
            'address', 'origin_key', 'contract_name', 'usage_category',
            '_comment', '_source', 'attester', 'is_protocol_contract_likely',
            'confidence', 'txcount', 'gas_eth', 'avg_daa', 'rel_cost',
            'github_found',
        }
        df = df.drop(columns=[c for c in df.columns if c not in AIRTABLE_COLS])

        at.push_to_airtable(table, df)
        logger.info(f"[Airtable] Wrote {len(df)} rows to 'Label Pool Automated'.")
    except Exception as e:
        logger.error(f"[Airtable] Write failed: {e}")


# ─── Main pipeline ────────────────────────────────────────────────────────────

async def run_pipeline(args):
    # 1. Fetch contracts
    if args.input_json:
        if _HAS_DB:
            try:
                _load_chain_config(DbConnector().engine)
            except Exception as e:
                logger.warning(f"Could not load chain config from DB: {e}")
        contracts = load_contracts_from_json(args.input_json)
    else:
        contracts = fetch_unlabeled_contracts(
            days=args.days,
            origin_keys=args.chains or None,
            max_contracts=args.max_contracts,
            min_txcount=getattr(args, 'min_txcount', 0),
            use_v2=getattr(args, 'use_v2_fetch', True),
            per_chain_limit=getattr(args, 'per_chain_limit', 0),
        )

    if not contracts:
        logger.info("No contracts to process.")
        return

    inject_chain_median_daa(contracts, days=getattr(args, 'days', 7))
    logger.info(f"Processing {len(contracts)} contracts (concurrency={args.concurrency})")

    # 2. Build API clients
    ssl_ctx = _ssl_ctx()
    connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit=20)

    async with aiohttp.ClientSession(connector=connector) as session:
        blockscout = BlockscoutAPI_Async(
            session=session,
            api_keys=Config.BLOCKSCOUT_API_KEYS,
            chain_config=Config.CHAIN_EXPLORER_CONFIG,
        )
        github = GitHubAPI_Async(
            session=session,
            headers=Config.get_github_headers(),  # App token (30/min) if configured, else personal (10/min)
            excluded_repos=Config.EXCLUDED_REPOS,
        )
        tenderly = TenderlyAPI_Async(session=session)

        # Pre-warm OLI category cache
        categories = await _oli_cache.get(session)
        valid_ids = categories.get('valid_ids', [])

        # 3 + 4. Enrich and classify in pipeline: classify each contract as soon as
        # its enrichment completes rather than waiting for the full batch to finish.
        enrich_sem = asyncio.Semaphore(args.concurrency)
        # Limit GitHub calls to 2 concurrent to avoid hitting the 30 req/min rate limit
        github_sem = asyncio.Semaphore(2)
        # Cap Gemini concurrency at 3 — bursts of 5+ cause model-side throttling (~4.9s latency)
        classify_sem = asyncio.Semaphore(min(args.concurrency, 3))

        async def _safe_enrich(c: dict) -> dict:
            try:
                return await enrich_contract(c, blockscout, github, tenderly, enrich_sem, github_sem)
            except Exception as e:
                logger.error(f"Enrichment error for {c.get('address')}: {e}")
                c['blockscout'] = {}
                c['github'] = {'has_valid_repo': False}
                c['traces'] = []
                return c

        async def _classify_one(contract: dict) -> dict:
            async with classify_sem:
                label = await classify_contract(
                    address=contract['address'],
                    origin_key=contract['origin_key'],
                    metrics=contract.get('metrics', {}),
                    blockscout=contract.get('blockscout', {}),
                    github=contract.get('github', {}),
                    traces=contract.get('traces', []),
                    session=session,
                    address_logs=contract.get('address_logs', []),
                    token_transfers=contract.get('token_transfers', []),
                )
                contract['label'] = label
                logger.info(
                    f"Labeled {contract['address']} ({contract['origin_key']}): "
                    f"{label.get('usage_category')} / {label.get('contract_name')} "
                    f"(conf={label.get('confidence', 0):.2f})"
                )
                return contract

        # Start all enrichment tasks, then classify each one as it finishes
        enrich_tasks = [asyncio.create_task(_safe_enrich(c)) for c in contracts]
        classify_tasks = []
        for coro in asyncio.as_completed(enrich_tasks):
            enriched = await coro
            classify_tasks.append(asyncio.create_task(_classify_one(enriched)))

        labeled = await asyncio.gather(*classify_tasks, return_exceptions=True)

        final = []
        for r in labeled:
            if isinstance(r, Exception):
                logger.error(f"Classification error: {r}")
            else:
                final.append(r)

    # 5. Build attestation rows and write output
    # Use v2 builder (sentinel + Ambiguous Contract rename) when use_v2_fetch is set
    if getattr(args, 'use_v2_fetch', True):
        rows = build_attestation_rows_v2(final, valid_ids)
    else:
        rows = build_attestation_rows(final, valid_ids)
    json_path, csv_path = write_output(rows, args.output_dir)

    # TODO(contract_label_features): write all classified contracts (including low-confidence ones
    # below the 0.5 attestation threshold) to public.contract_label_features via a new
    # db_connector.upsert_contract_label_features() method. This must happen BEFORE OLI attestation
    # so every run is captured regardless of whether the contract clears the confidence threshold.
    #
    # Build feature rows from `final` (the list of enriched+labeled contract dicts):
    #
    #   feature_rows = [_build_feature_row(c) for c in final if c.get('label')]
    #   db_connector.upsert_contract_label_features(feature_rows)
    #
    # _build_feature_row(contract) pulls from:
    #   contract['blockscout']      → is_verified, is_proxy, impl_address, impl_name, impl_verified, blockscout_name
    #   contract['github']          → has_github_repo, github_repo_count
    #   contract['metrics']         → txcount, gas_eth, avg_daa, avg_gas_per_tx, rel_cost, success_rate, day_range
    #   contract['token_transfers'] → bs_token_transfers (raw jsonb)
    #   contract['label']           → all ai_* columns + all pre-computed signal columns
    #                                 (requires the TODO in ai_classifier.py to be done first)
    #   _is_protocol_likely(...)    → ai_owner_project_likely (bool)
    #
    # See docs/contract_label_features_schema.md for the full column list.

    # Summary
    by_category: dict[str, int] = {}
    for r in rows:
        by_category[r['usage_category']] = by_category.get(r['usage_category'], 0) + 1

    logger.info("=" * 60)
    logger.info(f"DONE — {len(rows)} contracts labeled")
    logger.info(f"Output: {json_path}")
    logger.info(f"Output: {csv_path}")
    logger.info("Category breakdown:")
    for cat, count in sorted(by_category.items(), key=lambda x: -x[1]):
        logger.info(f"  {cat}: {count}")
    logger.info("=" * 60)

    # Step 6 — OLI off-chain attestation (only if --attest flag is set)
    #
    # Two-attester model:
    # - OLI_automated_labeler         → general attester (dex, bridge, lending, etc.)
    # - OLI_automated_labeler_trading → trading specialist (higher contract_name trust)
    #
    # Trading contract names are deterministic from human heuristics (PRIVATE caller
    # diversity + low success_rate + gas pattern → MEVBot/PrivateRouter/etc.) so they
    # warrant a separate trust list entry with higher contract_name confidence.
    attested_rows = []
    if getattr(args, 'attest', False):
        from oli import OLI
        oli_general = OLI(private_key=os.getenv("OLI_automated_labeler"))
        trading_pk = os.getenv("OLI_automated_labeler_trading")
        oli_trading = OLI(private_key=trading_pk) if trading_pk else None
        if not oli_trading:
            logger.warning(
                "OLI_automated_labeler_trading not set — trading contracts will use "
                "the general attester. Set the env var and add its address to the trust list."
            )

        confidence_threshold = getattr(args, 'confidence_threshold', 0.3)
        skipped = 0
        general_labels, trading_labels = [], []
        general_rows, trading_rows = [], []

        for row in rows:
            if row.get('confidence', 0) < confidence_threshold:
                skipped += 1
                continue
            reasoning = row.get('reasoning', '') or ''
            confidence = row.get('confidence')
            comment = f"confidence: {confidence} | {reasoning}" if reasoning else None
            tags = {k: v for k, v in {
                'contract_name': row.get('contract_name') or None,
                'owner_project': row.get('owner_project') or None,
                'usage_category': row.get('usage_category') or None,
                '_source': row.get('source') or None,
                '_comment': comment,
            }.items() if v}
            entry = {'address': row['address'], 'chain_id': row['chain_id'], 'tags': tags}

            is_trading = row.get('usage_category') == 'trading'
            if is_trading and oli_trading:
                trading_labels.append(entry)
                trading_rows.append(row)
            else:
                general_labels.append(entry)
                general_rows.append(row)

        def _submit_batch(client, labels, rows, label):
            if not labels:
                return []
            try:
                response = client.submit_label_bulk(labels)
                uids = response.get('uids') or []
                logger.info(
                    f"[{label}] Bulk attestation: {len(uids)} attested, "
                    f"accepted={response.get('accepted')}, "
                    f"duplicates={response.get('duplicates')}, "
                    f"failed_validation={response.get('failed_validation')}"
                )
                for row, uid in zip(rows, uids):
                    logger.info(f"  {row['address']} on {row['chain_id']}: uid={uid}")
                return rows
            except Exception as e:
                logger.error(f"[{label}] Bulk attestation failed: {e}")
                return []

        attested_rows += _submit_batch(oli_general, general_labels, general_rows, "general")
        attested_rows += _submit_batch(
            oli_trading if oli_trading else oli_general,
            trading_labels, trading_rows,
            "trading" if oli_trading else "trading→general"
        )
        logger.info(f"Attestation complete — {len(attested_rows)} attested, {skipped} skipped (low confidence)")

    # Step 7 — Write all attested rows to Airtable for visibility and owner_project assignment.
    if attested_rows:
        _write_attested_to_airtable(attested_rows)
        protocol_likely = sum(1 for r in attested_rows if r.get('owner_project') == 'protocol-contract-likely')
        logger.info(f"Airtable write: {len(attested_rows)} rows ({protocol_likely} protocol-likely, {len(attested_rows) - protocol_likely} trading/other)")


# ─── Airtable re-classification ──────────────────────────────────────────────

async def reclassify_from_airtable(args):
    """Fetch all records from 'Label Pool Automated', re-run enrichment + classifier,
    write new_contract_name / new_usage_category / new_comment back to Airtable,
    and save an attestation-ready JSON file (no actual attestation).
    """
    try:
        import pandas as pd
        from pyairtable import Api
        import src.misc.airtable_functions as at
    except ImportError as e:
        logger.error(f"[Reclassify] Missing dependency: {e}")
        return

    if _HAS_DB:
        try:
            _load_chain_config(DbConnector().engine)
        except Exception as e:
            logger.warning(f"[Reclassify] Could not load chain config from DB: {e}")

    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("AIRTABLE_BASE_ID")
    if not api_key or not base_id:
        logger.error("[Reclassify] AIRTABLE_API_KEY or AIRTABLE_BASE_ID not set")
        return

    api = Api(api_key)
    table = api.table(base_id, 'Label Pool Automated')

    logger.info("[Reclassify] Fetching records from 'Label Pool Automated'…")
    df = at.read_airtable(table)
    if df.empty:
        logger.info("[Reclassify] Table is empty — nothing to do.")
        return
    logger.info(f"[Reclassify] Found {len(df)} records")

    # Resolve origin_key linked-record IDs → caip2 → plain origin_key string
    df_chains = at.read_airtable(api.table(base_id, 'Chains'))
    df_chains_idx = df_chains[['id', 'caip2']].set_index('id')

    # Reverse map: caip2 → origin_key (e.g. "eip155:8453" → "base")
    caip2_to_origin: dict[str, str] = {v: k for k, v in CHAIN_TO_EIP155.items()}

    def _resolve_origin_key(val) -> Optional[str]:
        rec_id = val[0] if isinstance(val, list) and val else val
        if not rec_id or rec_id not in df_chains_idx.index:
            return None
        caip2 = df_chains_idx.loc[rec_id, 'caip2']
        return caip2_to_origin.get(caip2, caip2)

    df['origin_key_str'] = df['origin_key'].apply(_resolve_origin_key)

    contracts = []
    for _, row in df.iterrows():
        addr = row.get('address')
        ok = row.get('origin_key_str')
        rec_id = row.get('id')
        if not addr or not ok:
            logger.warning(f"[Reclassify] Skipping record {rec_id}: address={addr!r} origin_key={ok!r}")
            continue
        txcount = int(row['txcount']) if 'txcount' in row and pd.notna(row.get('txcount')) else 0
        gas_eth = float(row['gas_eth']) if 'gas_eth' in row and pd.notna(row.get('gas_eth')) else 0.0
        avg_daa = float(row.get('avg_daa') or row.get('daa') or 0)
        rel_cost_raw = row.get('rel_cost')
        rel_cost = float(rel_cost_raw) if rel_cost_raw is not None and pd.notna(rel_cost_raw) else 0.0
        sr_raw = row.get('success_rate')
        success_rate = float(sr_raw) if sr_raw is not None and pd.notna(sr_raw) else None
        contracts.append({
            'address': addr,
            'origin_key': ok,
            '_airtable_id': rec_id,
            'metrics': {
                'txcount': txcount,
                'gas_eth': gas_eth,
                'avg_daa': avg_daa,
                'rel_cost': rel_cost,
                'avg_gas_per_tx': round(gas_eth / txcount, 8) if txcount else 0.0,
                'day_range': 7,
                'success_rate': success_rate,
            },
        })

    if not contracts:
        logger.info("[Reclassify] No valid contracts to process.")
        return
    inject_chain_median_daa(contracts, days=7)
    logger.info(f"[Reclassify] Processing {len(contracts)} contracts (concurrency={args.concurrency})")

    ssl_ctx = _ssl_ctx()
    connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit=20)

    async with aiohttp.ClientSession(connector=connector) as session:
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

        categories = await _oli_cache.get(session)
        valid_ids = categories.get('valid_ids', [])

        enrich_sem = asyncio.Semaphore(args.concurrency)
        github_sem = asyncio.Semaphore(2)
        classify_sem = asyncio.Semaphore(min(args.concurrency, 3))

        async def _safe_enrich(c: dict) -> dict:
            try:
                return await enrich_contract(c, blockscout, github, tenderly, enrich_sem, github_sem)
            except Exception as e:
                logger.error(f"[Reclassify] Enrichment error for {c.get('address')}: {e}")
                c['blockscout'] = {}
                c['github'] = {'has_valid_repo': False}
                c['traces'] = []
                return c

        async def _classify_one(contract: dict) -> dict:
            async with classify_sem:
                label = await classify_contract(
                    address=contract['address'],
                    origin_key=contract['origin_key'],
                    metrics=contract.get('metrics', {}),
                    blockscout=contract.get('blockscout', {}),
                    github=contract.get('github', {}),
                    traces=contract.get('traces', []),
                    session=session,
                    address_logs=contract.get('address_logs', []),
                    token_transfers=contract.get('token_transfers', []),
                )
                contract['label'] = label
                logger.info(
                    f"[Reclassify] {contract['address']} ({contract['origin_key']}): "
                    f"{label.get('usage_category')} / {label.get('contract_name')} "
                    f"(conf={label.get('confidence', 0):.2f})"
                )
                return contract

        enrich_tasks = [asyncio.create_task(_safe_enrich(c)) for c in contracts]
        classify_tasks = []
        for coro in asyncio.as_completed(enrich_tasks):
            enriched = await coro
            classify_tasks.append(asyncio.create_task(_classify_one(enriched)))

        labeled = await asyncio.gather(*classify_tasks, return_exceptions=True)

    final = []
    for r in labeled:
        if isinstance(r, Exception):
            logger.error(f"[Reclassify] Classification error: {r}")
        else:
            final.append(r)

    # Write new_* fields back to Airtable
    updates = []
    for c in final:
        rec_id = c.get('_airtable_id')
        if not rec_id:
            continue
        label = c.get('label', {})
        conf = label.get('confidence', 0.0)
        reasoning = (label.get('reasoning', '') or '').strip()
        new_comment = f"confidence: {conf:.2f} | {reasoning}" if reasoning else f"confidence: {conf:.2f}"
        log_signals = decode_log_signals(c.get('address_logs', []))
        log_signals['has_valid_repo'] = c.get('github', {}).get('has_valid_repo', False)
        protocol_likely = _is_protocol_likely(label, c.get('blockscout', {}), log_signals, c.get('metrics', {}))
        updates.append({
            'id': rec_id,
            'fields': {
                'new_contract_name': label.get('contract_name', '') or '',
                'new_usage_category': label.get('usage_category', '') or '',
                'new_comment': new_comment,
                'is_protocol_contract_likely': protocol_likely,
            },
        })

    if updates:
        logger.info(f"[Reclassify] Writing {len(updates)} updates to Airtable…")

        # Map usage_category string → Airtable Sub Categories record ID (linked field)
        cat_table = api.table(base_id, 'Sub Categories')
        df_cat = at.read_airtable(cat_table)
        cat_to_id: dict[str, str] = df_cat.set_index('category_id')['id'].to_dict()

        for u in updates:
            raw_cat = u['fields'].get('new_usage_category', '')
            rec_id_cat = cat_to_id.get(raw_cat)
            u['fields']['new_usage_category'] = [rec_id_cat] if rec_id_cat else []

        # batch_update handles batching in chunks of 10 internally
        table.batch_update(updates)
        logger.info("[Reclassify] Airtable write complete.")

    # Save attestation-ready file (no attestation submitted)
    rows = build_attestation_rows(final, valid_ids)
    json_path, csv_path = write_output(rows, args.output_dir, prefix="reclassified")

    by_category: dict[str, int] = {}
    for r in rows:
        by_category[r['usage_category']] = by_category.get(r['usage_category'], 0) + 1

    logger.info("=" * 60)
    logger.info(f"[Reclassify] DONE — {len(rows)} contracts re-labeled")
    logger.info(f"[Reclassify] Attestation file: {json_path}")
    logger.info("[Reclassify] Category breakdown:")
    for cat, count in sorted(by_category.items(), key=lambda x: -x[1]):
        logger.info(f"  {cat}: {count}")
    logger.info("=" * 60)


# ─── CLI ─────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Automatically label unlabeled smart contracts using DB + AI"
    )
    p.add_argument(
        '--chains', nargs='+', default=None,
        help='origin_key(s) to process (default: all api_in_main chains)'
    )
    p.add_argument(
        '--days', type=int, default=7,
        help='Day range for unlabeled query (default: 7)'
    )
    p.add_argument(
        '--max-contracts', type=int, default=50,
        help='Max contracts to label in total (default: 50)'
    )
    p.add_argument(
        '--per-chain-limit', type=int, default=0, dest='per_chain_limit',
        help='Max contracts per chain (default: 0 = no limit)'
    )
    p.add_argument(
        '--concurrency', type=int, default=5,
        help='Max parallel contracts to process (default: 5)'
    )
    p.add_argument(
        '--min-txcount', type=int, default=100,
        help='Skip contracts below this txcount (default: 100)'
    )
    p.add_argument(
        '--output-dir', default='./output',
        help='Directory to write JSON/CSV output (default: ./output)'
    )
    p.add_argument(
        '--input-json', default=None,
        help='Path to a pre-exported JSON contract list (skips DB fetch)'
    )
    p.add_argument(
        '--attest', action='store_true', default=False,
        help='Submit off-chain OLI attestations after labeling'
    )
    p.add_argument(
        '--confidence-threshold', type=float, default=0.3,
        help='Minimum confidence to attest (default 0.5)'
    )
    p.add_argument(
        '--reclassify-airtable', action='store_true', default=False,
        help=(
            'Fetch all records from Airtable "Label Pool Automated", re-run enrichment + '
            'classifier, write new_contract_name / new_usage_category / new_comment back '
            'to those records, and save an attestation-ready JSON (no attestation submitted)'
        )
    )
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()
    if args.reclassify_airtable:
        asyncio.run(reclassify_from_airtable(args))
    else:
        asyncio.run(run_pipeline(args))
