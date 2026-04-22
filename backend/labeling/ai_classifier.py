"""
AI Contract Classifier
======================
Uses Gemini 2.0 Flash (via google-genai SDK) with function calling / structured output
to assign a short contract_name and OLI usage_category to unlabeled smart contracts.

OLI categories are fetched from GitHub and filtered to GTP-supported sub_categories.
Results are cached for 1 hour.
"""

import asyncio
import aiohttp
import logging
import os
import re
import time
import ssl
import yaml
import json
from typing import Optional

from google import genai
from google.genai import types as genai_types

logger = logging.getLogger(__name__)

# ─── URLs ────────────────────────────────────────────────────────────────────
OLI_USAGE_CATEGORY_URL = (
    "https://raw.githubusercontent.com/openlabelsinitiative/OLI/refs/heads/main"
    "/1_label_schema/tags/valuesets/usage_category.yml"
)
GTP_MASTER_URL = "https://api.growthepie.com/v1/master.json"

CATEGORIES_CACHE_TTL = 3600  # 1 hour
GEMINI_MODEL = "gemini-2.5-flash"


def _ssl_ctx():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


# ─── OLI Category Cache ───────────────────────────────────────────────────────

class OLICategoryCache:
    """Fetches and caches OLI usage categories, filtered to GTP-supported ones."""

    def __init__(self):
        self._cache: Optional[dict] = None
        self._cache_time: float = 0

    async def get(self, session: aiohttp.ClientSession) -> dict:
        """Return {'context': str, 'valid_ids': list[str]}. Fetches if stale."""
        if self._cache and (time.time() - self._cache_time) < CATEGORIES_CACHE_TTL:
            return self._cache

        try:
            async with session.get(OLI_USAGE_CATEGORY_URL, ssl=_ssl_ctx(), timeout=aiohttp.ClientTimeout(total=15)) as r:
                oli_text = await r.text()
            oli_data = yaml.safe_load(oli_text)
            all_cats = oli_data.get('categories', [])
        except Exception as e:
            logger.warning(f"[Classifier] Failed to fetch OLI categories: {e}")
            all_cats = []

        # Filter to GTP-supported sub_categories
        supported_ids: Optional[set] = None
        try:
            async with session.get(GTP_MASTER_URL, ssl=_ssl_ctx(), timeout=aiohttp.ClientTimeout(total=15)) as r:
                gtp = await r.json(content_type=None)
            subs = gtp.get('blockspace_categories', {}).get('sub_categories', {})
            if subs:
                supported_ids = set(subs.keys())
        except Exception as e:
            logger.debug(f"[Classifier] Could not fetch GTP master for category filter: {e}")

        categories = (
            [c for c in all_cats if c.get('category_id') in supported_ids]
            if supported_ids else all_cats
        )

        context_lines = []
        valid_ids = []
        for c in categories:
            cid = c.get('category_id', '')
            name = c.get('name', '')
            desc = c.get('description', '')
            context_lines.append(f"- {cid}: {name}" + (f" — {desc}" if desc else ""))
            valid_ids.append(cid)

        self._cache = {
            'context': "\n".join(context_lines),
            'valid_ids': valid_ids,
        }
        self._cache_time = time.time()
        logger.info(f"[Classifier] Loaded {len(valid_ids)} OLI categories")
        return self._cache


# Module-level singleton cache
_oli_cache = OLICategoryCache()


# ─── 4byte Lookup ────────────────────────────────────────────────────────────

_4BYTE_DICT: dict | None = None   # lazy-loaded


def _load_4byte() -> dict:
    global _4BYTE_DICT
    if _4BYTE_DICT is not None:
        return _4BYTE_DICT
    import pickle
    from pathlib import Path
    candidates = [
        Path(__file__).resolve().parent.parent / "four_byte_lookup.pkl",  # backend/
        Path(__file__).resolve().parent / "four_byte_lookup.pkl",         # labeling/
    ]
    for p in candidates:
        if p.exists():
            try:
                with open(p, 'rb') as f:
                    _4BYTE_DICT = pickle.load(f)
                logger.info(f"[4byte] Loaded {len(_4BYTE_DICT):,} selectors from {p}")
                return _4BYTE_DICT
            except Exception as e:
                logger.warning(f"[4byte] Failed to load {p}: {e}")
    logger.warning("[4byte] four_byte_lookup.pkl not found — selector resolution disabled")
    _4BYTE_DICT = {}
    return _4BYTE_DICT


def resolve_selector(selector: str) -> str:
    """Return first matching function name for a 4-byte selector, or '' if unknown."""
    if not selector or len(selector) < 10:
        return ''
    key = selector[:10].lower()
    if not key.startswith('0x'):
        key = '0x' + key
    matches = _load_4byte().get(key)
    if not matches:
        return ''
    # Return just the function name (strip params for brevity)
    full = matches[0]
    return full.split('(')[0] if '(' in full else full


# ─── Trace Summarizer ─────────────────────────────────────────────────────────

def _format_trace_for_prompt(trace: dict, index: int) -> str:
    """Format a trace summary as a protocol-level call graph for the classifier prompt.

    Token budget: no tx hashes; cap internal (JUMPDEST) calls per contract to 3;
    show at most 20 lines total per trace.
    """
    if not trace:
        return f"Trace {index+1}: unavailable"

    status = trace.get('status', 'unknown')
    gas = trace.get('gas_used')
    raw_entry = trace.get('entry_method', 'unknown')
    # Resolve raw selector (e.g. '0x3850c7bd') → function name if possible
    entry = resolve_selector(raw_entry) or raw_entry
    err = trace.get('error_message', '')

    lines = [f"Trace {index+1}  status={status}  gas={gas:,}  entry={entry}"]
    if err:
        lines.append(f"  ERROR: {err}")

    proto_calls = trace.get('protocol_calls', [])
    if proto_calls:
        lines.append("  Call graph:")
        internal_counts: dict[str, int] = {}  # contract → count of internal calls shown
        shown = 0
        for c in proto_calls:
            if shown >= 20:
                lines.append("    ...")
                break
            contract = c.get('contract', '?')
            fn = c.get('function', '') or resolve_selector(c.get('selector', ''))
            ct = c.get('call_type', '')

            # Collapse repetitive internal JUMPDEST math calls (e.g. mulDiv, getSqrtRatioAtTick)
            if ct == 'internal':
                internal_counts[contract] = internal_counts.get(contract, 0) + 1
                if internal_counts[contract] > 3:
                    continue  # suppress after 3 per contract

            prefix = 'STATIC' if ct == 'STATICCALL' else ('DELEGATE' if ct == 'DELEGATECALL' else '')
            key_arg = c.get('key_arg', '')
            fn_str = f".{fn}({key_arg})" if fn and key_arg else (f".{fn}()" if fn else '')
            # Drop unresolved hex addresses — show function if available, else skip
            if contract.startswith('0x'):
                if not fn:
                    continue  # raw address with no function name = noise
                contract = '[unknown]'
            lines.append(f"    {prefix}{contract}{fn_str}")
            shown += 1
    else:
        contracts = trace.get('involved_contracts', [])
        if contracts:
            parts = [c['name'] or c['address'][:10] for c in contracts[:8]]
            lines.append(f"  Contracts touched: {', '.join(parts)}")

    return "\n".join(lines)


# ─── Chain mapping ────────────────────────────────────────────────────────────
# Populated at runtime from sys_main_conf by automated_labeler.py / classify_contract_tool.py.
# Do not add hardcoded entries here — use load_chain_to_eip155() instead.

CHAIN_TO_EIP155: dict[str, str] = {}


# ─── Log Signal Decoder ───────────────────────────────────────────────────────

# topic0 → (human_name, category_hint)
_KNOWN_TOPIC0: dict[str, tuple[str, str]] = {
    # Access control
    "0x2f8788117e7eff1d82e926ec794901d17c78024a50270940304540a733656f0d": ("RoleGranted",          "access_control"),
    "0xf6391f5c32d9c69d2a47ea670b442974b53935d1edc7fd64eb21e047a839171b": ("RoleRevoked",          "access_control"),
    "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0": ("OwnershipTransferred", "access_control"),
    # ERC-20 / tokens
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": ("Transfer",             "token"),
    "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925": ("Approval",             "token"),
    # ERC-721 / NFT
    "0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31": ("ApprovalForAll",       "nft"),
    # DEX / AMM
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822": ("Swap(V2)",             "dex"),
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67": ("Swap(V3)",             "dex"),
    "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde": ("Mint(V3)",             "dex"),
    "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c": ("Burn(V3)",             "dex"),
    "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118": ("PoolCreated",          "dex"),
    # Bridge
    "0x02a52367d10742d8032712c1bb8e0144ff1ec5ffda1ed7d70bb05a2744955054": ("MessagePassed",        "bridge"),
    "0xcb0f7ffd78f9aee47a248fae8db181db6eee628b008a4b35be07491e9b73d2a4": ("SentMessage",         "bridge"),
    "0xb3813568d9991fc951961fcb4c784893574240a28925604d09fc577c55bb7c32": ("TransactionDeposited", "bridge"),
    "0x4641df4a962071e12719d8c8c8e5ac7fc4d97b927346a3d7a335b1f7517e133c": ("MessageRelayed",      "bridge"),
    # ERC-4337
    "0x49628fd1471006c1482da88028e9ce4dbb080b815c9b0344d39e5a8e6ec1419f": ("UserOperationEvent",  "erc4337"),
    "0xd51a9c61267aa6196961883ecf5ff2da6619c37dac0fa92122513fb32c032d2d": ("AccountDeployed",     "erc4337"),
    # Staking
    "0x9e71bc8eea02a63969f509818f2dafb9254532904319f9dbda79b67bd34a5f3d": ("Staked",              "staking"),
    "0x7084f5476618d8e60b11ef0d7d3f06914655adb8793e28ff7f018d4c76d505d5": ("Withdrawn",           "staking"),
    # Governance
    "0x7d84a6263ae0d98d3329bd7b46bb4e8d6f98cd35a7adb45c274c8b7fd5ebd5e0": ("ProposalCreated",     "governance"),
    "0xb8e138887d0aa13bab447e82de9d5c1777041ecd21ca36ba824ff1e6c07ddda4": ("VoteCast",            "governance"),
}


def decode_log_signals(address_logs: list[dict]) -> dict:
    """Decode raw address logs into classifier-ready signals.

    Returns:
        {
          'event_counts': {'RoleGranted': 3, 'Swap(V3)': 12, ...},
          'category_hints': ['access_control', 'dex'],   # deduplicated, ordered by freq
          'has_access_control': bool,
          'has_token_transfers': bool,
          'has_swaps': bool,
          'has_bridge_events': bool,
          'has_erc4337': bool,
          'has_staking': bool,
          'has_governance': bool,
          'summary': str,   # one-line for prompt
        }
    """
    event_counts: dict[str, int] = {}
    hint_counts: dict[str, int] = {}

    for log in address_logs:
        t0 = (log.get('topic0') or '').lower()
        info = _KNOWN_TOPIC0.get(t0)
        if info:
            name, hint = info
            event_counts[name] = event_counts.get(name, 0) + 1
            hint_counts[hint] = hint_counts.get(hint, 0) + 1

    hints_ordered = [h for h, _ in sorted(hint_counts.items(), key=lambda x: -x[1])]

    result = {
        'event_counts': event_counts,
        'category_hints': hints_ordered,
        'has_access_control': 'access_control' in hint_counts,
        'has_token_transfers': 'token' in hint_counts,
        'has_swaps': 'dex' in hint_counts,
        'has_bridge_events': 'bridge' in hint_counts,
        'has_erc4337': 'erc4337' in hint_counts,
        'has_staking': 'staking' in hint_counts,
        'has_governance': 'governance' in hint_counts,
    }

    if event_counts:
        top = sorted(event_counts.items(), key=lambda x: -x[1])[:5]
        result['summary'] = ', '.join(f"{n}×{c}" for n, c in top)
    else:
        result['summary'] = 'no known events detected'

    return result


# ─── JSON repair ─────────────────────────────────────────────────────────────

def _extract_json_fields(text: str) -> dict | None:
    """Regex-based field extraction for unrecoverable Gemini JSON responses.

    Handles truncation and unescaped quotes in the reasoning field.
    Returns a dict with whatever fields could be recovered, or None if none found.
    """
    out: dict = {}
    for field in ('contract_name', 'usage_category'):
        m = re.search(rf'"{field}"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"', text)
        if m:
            out[field] = m.group(1)
    m = re.search(r'"confidence"\s*:\s*([0-9]*\.?[0-9]+)', text)
    if m:
        out['confidence'] = float(m.group(1))
    # Reasoning: take everything after the opening quote, stop at closing `"}` or end
    m = re.search(r'"reasoning"\s*:\s*"(.*)', text, re.DOTALL)
    if m:
        raw_reasoning = m.group(1)
        # Strip trailing JSON cruft — stop at first unescaped `"` followed by `}` or end
        raw_reasoning = re.split(r'(?<!\\)"\s*[},]', raw_reasoning)[0]
        out['reasoning'] = raw_reasoning[:600].replace('\n', ' ')
    return out if out else None


# ─── Gemini client singleton ─────────────────────────────────────────────────

_gemini_client: "genai.Client | None" = None


def _get_gemini_client() -> "genai.Client":
    global _gemini_client
    if _gemini_client is None:
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY not set")
        _gemini_client = genai.Client(api_key=api_key)
    return _gemini_client


# ─── Static system instruction (sent once, not repeated per contract) ─────────

_SYSTEM_INSTRUCTION = """You are a smart contract protocol analyst. Classify the contract described below by following the decision paths IN ORDER. Stop at the first path that applies.

## HOW TO CLASSIFY

Two primary sources of truth, in order of reliability:
1. **Verified name** — if the contract is source-verified with a real name, read it as a human would. "GridMining" is gaming. "LendingPool" is lending. "BridgeRouter" is bridge. Trust your semantic understanding of what the name describes. Use traces to confirm or disambiguate, not to override.
2. **Trace behavior** — what the contract actually does: what it calls, what events it emits, what state it changes. Classify by function.

Numerical signals (success_rate, DAA/tx, rel_cost) are tiebreakers. They shift confidence but do not override clear semantic or trace evidence.

## DECISION PATHS (follow in order, stop at first match)

### PATH A — Verified + Named Contract
IF source_verified=True AND blockscout_name != "unknown":
  → contract_name = exact Blockscout name
  → Classify by the semantic meaning of the verified name. Read it as a human — you know what protocols do.
  → Use traces to disambiguate when the name alone is genuinely unclear. VRF calls (Chainlink VRF, Pyth) → gaming. NFT interactions without marketplace → gaming or non_fungible_tokens.
  → The verified name overrides MEV/bot heuristics — do NOT reclassify a named protocol as trading because callers are private or success_rate is low.
  → Protocol-specific corrections (the name sounds like X but IS actually Y):
      Permit2, Permit3, AllowanceHolder → developer_tools  (approval middleware, not a DEX router)
      GPv2Settlement, CoWSwap*, CowSwap*, BatchSettlement → dex  (batch DEX settlement, not a trading bot)
      Relay*Router, RelayRouter, RelayBridge, RelayReceiver, RelayDepository → bridge
      Rango*, RangoDiamond → bridge  (bridge aggregator, not dex)
      *DVN, *Dvn → cc_communication  (LayerZero verifier network)
      MayanSwift*, Mayan*Bridge → bridge
      *Router without clear DEX context → examine trace, do NOT default to dex
  → Stablecoin token check (W6): if the contract name is a distribution/claim mechanism
    (Claim, DiamondClaim, Distributor, Airdrop, Vesting) AND traces show transfers of known
    stablecoin tokens (Tether, TetherToken, USDT, USDC, FiatToken, DAI, BUSD, TUSD, or any
    token name containing "USD" or "Stablecoin") → stablecoin, NOT fungible_tokens.

### PATH B — Proxy Delegating to Named Implementation
IF "this contract itself delegates to (named)" != "none":
  → This proxy IS that named implementation. Classify by what the delegate target does.
  → CLPool / UniswapV3Pool / PancakeV3Pool → dex
  → EntryPoint → erc4337
  → FiatToken / USDC / ERC20 → stablecoin or fungible_tokens
  → RangoDiamond, Rango* → bridge
  → Relay*Router, RelayRouter* → bridge
  → Endpoint, EndpointV2, LZEndpoint → bridge (LayerZero endpoint proxy)
  → Apply same protocol-specific corrections from PATH A for any ambiguous delegate names
  NOTE (W5): Generic proxy names (TransparentUpgradeableProxy, OptimizedTransparentUpgradeableProxy,
  ERC1967Proxy, etc.) are stripped before reaching this path — they are treated as "unknown" name.
  The DELEGATECALL target name IS available in traces as "this contract itself delegates to".
  Always prefer the delegate target name over the proxy wrapper name.

### PATH C — Calls Official DEX Router (depth ≤1)
IF "Direct calls into official DEX routers" list is non-empty:
  → This contract is ABOVE official routers in the stack — an aggregator, bot, or strategy
  EXCEPTION — verified contracts: do NOT apply the trading default.
    Verified contracts that call official routers are settlement/fulfillment infrastructure
    (RFQ market makers, solver contracts, intent-settlement). Prefer dex.
    Only assign trading if traces show explicit MEV patterns (very low success_rate + clear failed-tx-heavy volume).
  → Unverified contracts: default to trading unless strong evidence of a public aggregator:
    - Few unique callers (low DAA/tx) → trading
    - Many failed txs (low success_rate) → trading, lean high confidence
    - High DAA/tx + high success_rate → could be public aggregator → dex
    - Both low DAA/tx AND low success_rate → near-certain private trading

### PATH D — Calls AMM Pool Swap Functions Directly (depth ≤1)
IF "Direct calls into AMM pool swap functions" = True AND no_meaningful_identity = True:
  → Calls CLPool.swap / UniswapV3Pool.swap directly (not just price reads)
  NOTE: AMM pools list non-empty but swap = False → price reads only (slot0, getReserves). Do not use PATH D; fall through to PATH F or G.
  → PRIMARY: caller pattern — PRIVATE callers → trading; DIVERSE callers → dex
  → SECONDARY: success_rate < 50% → aggressive bot → trading; ≥70% + diverse → dex
  → TERTIARY (no caller data): low DAA/tx → trading; high DAA/tx → dex

### PATH E — EIP-1167 Minimal Clone (Empty Trace)
IF "this contract has raw-address DELEGATE" = True AND "all trace call graphs empty" = True:
  → Factory clone forwarding to unresolvable implementation
  → PRIVATE callers → trading executor; DIVERSE callers → dex infrastructure clone
  → Use txcount as a tiebreaker when caller data is unavailable: high volume → trading, low → dex
  → Confidence is inherently limited — cannot fully resolve without the implementation

### PATH F — Pure Price Scanner
IF trace contains ONLY STATICCALL/SLOAD ops AND calls are exclusively slot0/price-read functions
   (slot0, observe, getReserves, latestRoundData) across multiple pools AND no swap/transfer calls:
  → Hunts opportunities without executing — Searcher or PriceScanner
  → usage_category = trading
  → Distinguish from oracle consumers: oracle consumers call Chainlink feeds (EACAggregatorProxy);
    Searchers call AMM pool slot0 across many different pools to compare prices

### PATH O — Oracle Infrastructure
IF "Oracle function calls detected" list is non-empty:
  → This contract calls or delegates to oracle-specific functions (storkPublicKey, latestRoundData,
    updatePriceFeed, parsePriceFeedUpdates, etc.). These functions only exist on oracle infrastructure.
  → usage_category = oracle
  → Name: "[Protocol]Oracle", "[Protocol]PriceFeed", or "[Protocol]OracleProxy" depending on trace depth.
    If DELEGATECALL to oracle → it IS the oracle (proxy). If CALL → it USES the oracle (consumer — fall through if only 1 oracle call and other DeFi context dominates).
  → Override even for unverified contracts with PRIVATE callers — a single-operator oracle updater
    is still oracle infrastructure, not a trading bot.

### PATH G1 — Private Callers (PRIVATE diversity ≤20%)
IF caller_diversity_label = "PRIVATE" AND no prior path matched:
  **CRITICAL: This path applies even when traces are raw opcodes only and identity is unknown.
  Raw-opcode traces + PRIVATE callers + no identity IS the trading bot signature — intentionally unverified.
  Do NOT skip to G0 because traces are unreadable. If the label says PRIVATE, use G1.**
  EXCEPTION — verified contracts: do NOT apply trading defaults. Verified contracts with private
    callers are almost always protocol infrastructure (vault, liquidator, position manager), not MEV bots.
    Classify by trace content; only assign trading if traces explicitly show MEV patterns (raw slot0
    reads across many pools, failed-tx-heavy swaps).
  VERIFIED + DEX router calls: prefer dex — settlement or fulfillment infrastructure. Use trace entry
    function to name it (e.g. "RFQSettlement", "FulfillHelper"). Exception: very low success_rate or
    clear failed-tx patterns → trading still possible.
  VERIFIED + bridge calls: prefer bridge or cc_communication.
  → Unverified private callers: TRADING IS THE ONLY VALID CLASSIFICATION. Do not use other.
    A single operator running automated on-chain transactions is by definition a trading bot.
    Unverified bots intentionally leave no decoded traces — raw opcodes and no identity ARE the signal.
    "No specific trading signals" is not a valid reason to pick other over trading here.
    Name it based on available signals, but the category is always trading:
  - LP/gauge contracts (CLGauge, CLPool, NonfungiblePositionManager) + novel tokens in traces:
      ACTIVE REBALANCER (slot0 reads + _collect + burns/mints in same tx) → trading, "[Protocol]CLRebalancer"
      PASSIVE LP MANAGER (only deposit/withdraw/mint/burn, no slot0 reads) → trading, "[Protocol][Token]LPManager"
      Include token pair in name when identifiable.
  - Very low success_rate → near-certain MEV/arbitrage → trading, "MEVBot" or "ArbitrageBot"
  - High success_rate → efficient strategy → trading, "StrategyExecutor"
  - Raw opcodes only, no decoded calls → trading, "StrategyExecutor"
  - Truly undifferentiated (no useful signals at all) → trading, "StrategyExecutor"
  - AccessControl events: confirms permissioned automation, does NOT change category

### PATH G2 — Keeper Callers (KEEPER diversity 20–80%)
IF caller_diversity_label = "KEEPER" AND no prior path matched:
  EXCEPTION — verified contracts with named protocol interactions: classify by trace content.
    Verified keeper contracts are typically protocol infrastructure (liquidators, settlement, distributors).
  VERIFIED + DEX router calls: KEEPER + verified + DEX router = DEX settlement/aggregation signature
    (1inch resolvers, CoW solvers, RFQ market makers are all KEEPER callers). Prefer dex.
  VERIFIED + bridge calls: prefer bridge or cc_communication.
  → Unverified multi-operator permissioned system. Check explicit protocol signals FIRST before
    defaulting to trading. Apply these overrides in order even when unverified:
  1. "Direct calls into lending protocols" non-empty (Aave, Compound, Morpho, etc.)
     AND no slot0/price-read pattern → lending (or derivative for perp protocols)
     Name: "[Protocol]Liquidator" or "[Protocol]KeeperBot"
  2. "Direct calls into bridge protocols" non-empty (Stargate, LayerZero, Connext, etc.)
     OR bridge events (MessagePassed, SentMessage, PacketSent) → bridge or cc_communication
  3. ERC-4337 events (UserOperationEvent, AccountDeployed) → erc4337
  4. Governance events (ProposalCreated, VoteCast) → governance
  → After checking the above, if no protocol signal matched:
  **Raw opcodes only + no identity + KEEPER callers → trading, "StrategyExecutor". Do NOT use other.**
  Multiple operators running opaque automated strategies is a trading pattern, not "ambiguous".
  - LP/gauge contracts + novel tokens: same ACTIVE/PASSIVE rebalancer distinction as G1
  - AccessControl events + no identifiable protocol interactions → trading executor
  - Known non-trading protocol interactions in traces: classify by what it interacts with
  - No DeFi signals, specific name: classify by semantic meaning of name. Do NOT default to other.
  - Truly unclear: use trading as fallback, "StrategyExecutor"

### PATH G3 — Public Callers (PUBLIC diversity ≥80%)
IF caller_diversity_label = "PUBLIC" AND no prior path matched:
  → Many different callers — public-facing. Classify by what the contract does.
  - Known DEX interactions (swap events, pool calls) → dex
  - Named STATIC calls to EACAggregatorProxy/Chainlink feeds → oracle consumer
  - Pool-internal math (computeSwapStep, getSqrtRatioAtTick as INTERNAL nodes) → dex
  - Calls to LayerZero, Wormhole, Axelar, or other cross-chain messaging endpoints → cc_communication
  - Calls to VRF (Chainlink VRF, Pyth randomness) → gaming
  - Social/check-in pattern (simple state writes per user, no DeFi signals) → community
  - Calls to `liquidate`, `auction`, `seize`, or `settle` on lending/perp contracts → lending or derivative
  - No DeFi signals: classify by semantic meaning of the contract name. Do NOT default to other for a specific name.
  - Truly ambiguous name AND no usable signals → other
  - IMPORTANT: Named contracts in the DEEP trace are downstream callees — do NOT classify this contract as CLPool just because CLPool appears in the trace.

### PATH G0 — No Caller Data
IF caller_diversity_label = "UNKNOWN":
  **ONLY use this path if the data literally says "Caller diversity label: UNKNOWN". If it says PRIVATE, KEEPER, or PUBLIC — use G1, G2, or G3 instead, regardless of trace quality.**
  UNKNOWN means caller sampling failed — a data gap. Without caller data, use available signals carefully.
  → Decision tree:
  1. Non-trading events (bridge, NFT transfers, governance, staking) → classify by those signals
  2. success_rate clearly < 30% → near-certain MEV → trading, "MEVBot"
  3. Named protocol interactions in traces that imply a specific non-trading purpose → classify accordingly
  4. txcount > 300k AND all_trace_call_graphs_empty=True AND no events AND no meaningful identity
     → high-volume unidentifiable contract; lean trading, "StrategyExecutor"
     BUT: if DAA/tx signal says BROAD USER BASE (>5%) → override to other (public-facing, not a bot)
  5. All else → other, "AmbiguousContract"
  Note: DAA/tx=0.00% often means missing data, not confirmed automation — do not treat as strong trading signal alone.

---

## contract_name (max 50 chars)
Name what the contract IS or DOES — use protocol-aware names when traces reveal them.
- PATH A: use exact Blockscout name
- PATH B: delegate target name + "Proxy" suffix
- Unverified trading contracts: describe the behavior — "MEVBot", "ArbitrageBot", "PriceScanner", "[Protocol]CLRebalancer", "[Protocol][Token]LPManager"
- Unverified non-trading: derive from trace interactions — "OracleConsumer", "DEXAggregator", "BridgeCaller"
- When identity is truly unknown: "AmbiguousContract"
- NEVER use generic filler: "UnverifiedProxy", "UnverifiedContract", "Unknown", "MinimalProxy"

---

NOTE — Event signals are strong type indicators. A contract emitting Swap events IS a DEX pool.
AccessControl events alone = permissioned infrastructure, NOT a bot. Use AccessControl to confirm or upgrade confidence, not as a primary classifier.

For the reasoning field — required format: "PATH X[subpath]: [key signal=val, signal=val] → [chosen category]. Ruled out [alt] because [reason]."
Examples:
  "PATH G1: diversity=PRIVATE, success=99.9%, AccessControl, no swap events → trading. Ruled out dex (no pool calls)."
  "PATH C: router_calls=[SwapRouter02], DAA/tx=0.3%, success=18% → trading/MEVBot. Ruled out dex (above router stack)."
  "PATH A: verified=True, name=GridMining, VRF calls in trace → gaming. Ruled out other (name is specific)."
No filler, no articles, fragments OK. Always name the path. Always name what was ruled out."""


# ─── Classifier ───────────────────────────────────────────────────────────────

async def classify_contract(
    address: str,
    origin_key: str,
    metrics: dict,
    blockscout: dict,
    github: dict,
    traces: list[dict],
    session: aiohttp.ClientSession,
    address_logs: list[dict] | None = None,
    token_transfers: list[dict] | None = None,
    anthropic_client=None,  # kept for API compat, unused
) -> dict:
    """Classify a single contract using Gemini with function calling.

    Returns:
        {contract_name, usage_category, confidence, reasoning}
    """
    categories = await _oli_cache.get(session)
    valid_ids = categories['valid_ids']
    fallback_id = "other" if "other" in valid_ids else (valid_ids[0] if valid_ids else "other")

    chain_id_str = CHAIN_TO_EIP155.get(origin_key, f"eip155:unknown({origin_key})")

    # Build trace section
    if traces:
        formatted = [_format_trace_for_prompt(t, i) for i, t in enumerate(traces) if t]
        trace_section = "\n".join(formatted) if formatted else "No traces available."
    else:
        trace_section = "No trace data available."

    # Decode log signals
    log_signals = decode_log_signals(address_logs or [])

    # Generic proxy type names tell us nothing about what the contract does.
    # Treat them the same as having no name.
    _GENERIC_PROXY_NAMES = {
        'erc1967proxy', 'transparentupgradeableproxy', 'beaconproxy',
        'adminupgradeabilityproxy', 'uuproxy', 'proxy', 'upgradeable proxy',
        'minimal proxy', 'eip1167', 'clone',
        'optimizedtransparentupgradeableproxy',  # W5: OpenZeppelin optimized variant
    }
    raw_bs_name = blockscout.get('contract_name') or ''
    bs_name_is_generic = raw_bs_name.lower().replace(' ', '') in {n.replace(' ', '') for n in _GENERIC_PROXY_NAMES}
    bs_name = 'unknown' if (not raw_bs_name or bs_name_is_generic) else raw_bs_name
    is_verified = blockscout.get('is_verified', False)
    is_proxy = blockscout.get('is_proxy', False)
    has_repo = github.get('has_valid_repo', False)
    repo_url = github.get('repo_url', '')

    day_range = metrics.get('day_range', 7)
    txcount = metrics.get('txcount', 0)
    avg_daa = metrics.get('avg_daa', 0)
    chain_median_daa = metrics.get('chain_median_daa', 0.0)
    gas_eth = metrics.get('gas_eth', 0)
    rel_cost = metrics.get('rel_cost', 0)
    avg_gas_per_tx = metrics.get('avg_gas_per_tx', 0)
    success_rate = metrics.get('success_rate')  # None if not yet computed

    # Dynamic PUBLIC threshold: a contract must have avg_daa >= 5% of chain median
    # to qualify as truly public-facing. Floor of 3 handles chains with very low activity.
    # Contracts with high diversity ratio but tiny absolute DAA are small rotating operator sets.
    public_daa_threshold = max(chain_median_daa * 0.05, 3.0) if chain_median_daa > 0 else 10.0

    # ── Log enriched data summary ────────────────────────────────────────────
    success_rate_str = f"{success_rate:.1%}" if success_rate is not None else "n/a"
    logger.info(
        f"\n{'='*60}\n"
        f"[Input] {address} ({origin_key})\n"
        f"  Blockscout : name={bs_name!r}  verified={is_verified}  proxy={is_proxy}\n"
        f"  GitHub     : has_repo={has_repo}  url={repo_url or 'n/a'}\n"
        f"  Metrics    : txcount={txcount:,}  avg_daa={avg_daa:.1f}  "
        f"gas_eth={gas_eth:.6f}  rel_cost={rel_cost:.3f}x  "
        f"avg_gas_per_tx={avg_gas_per_tx:.8f}  success_rate={success_rate_str}\n"
        f"  Traces     : {len(traces)} traces\n"
        + (
            "\n".join(f"    {_format_trace_for_prompt(t, i)}" for i, t in enumerate(traces) if t)
            if traces else "    (none)"
        ) +
        f"\n{'='*60}"
    )

    daa_ratio = (avg_daa / txcount * 100) if txcount else 0

    # ── Pre-compute classification signals ────────────────────────────────────
    # Only look at depth-0/1 calls (direct callees of this contract) to avoid
    # confusing downstream proxy patterns with this contract's own behavior.
    _OFFICIAL_ROUTERS = {
        'swaprouter02', 'uniswapv2router02', 'universalrouter', 'swaprouter',
        'allowanceholder', 'permit2', 'basicselltopoolrouter', '0xsettler',
        # NOTE: permit2/allowanceholder here = callee detection (contract calls them = DEX signal)
        # PATH A keyword "Permit2 → developer_tools" handles the case where contract IS named Permit2
        'aggregationrouterv5', 'aggregationrouterv6', 'pancakerouter',
        'metaaggregationrouterv2', 'augustusswapper', 'transitswaprou',
        # Curve / Balancer
        'curvefi', 'curverouter', 'curvestableswap', 'balancervault', 'balancerv2vault',
        # Camelot / Ramses / Thruster / Ambient (L2 DEXes)
        'camelotrouter', 'ramsesrouter', 'thrusterrouter', 'ambientswap', 'ambientdex',
        # Aggregators
        'odosdexaggregator', 'openoceanexchange', 'lifiswap', 'lifidiamond',
        'socketgateway', 'kyberswap', 'kybernetwork', 'traderjoeswap',
        # Perps / hybrid routers
        'gmxrouter', 'gmxv2router', 'synthetixexchangerates',
    }
    _POOL_CONTRACTS = {
        'clpool', 'uniswapv3pool', 'pancakev3pool', 'pancakev2pool',
        'uniswapv2pair', 'aerodromeclpool', 'velodromeclpool',
        # Curve / Balancer pools
        'curvepool', 'curvev2pool', 'curvecryptoswap', 'balancerpool', 'balancerweightedpool',
        # L2 DEX pools
        'camelotpool', 'algebrapool', 'ramsespool', 'ambientpool',
        # Position managers (depth-1 calls here = LP management)
        'nonfungiblepositionmanager',
    }
    _LENDING_CONTRACTS = {
        'aavev3pool', 'aavev2lendingpool', 'comptroller', 'ctoken', 'cerc20',
        'comet', 'morphoblue', 'eulerv2', 'sparkpool', 'radiantlendingpool',
        'silo', 'exactlymarket', 'ionicpool',
    }
    _STAKING_CONTRACTS = {
        'clgauge', 'gauge', 'voter', 'rewarddistributor', 'stakingpool',
        'stakingcontract', 'rewardpool', 'masterchef', 'minichef',
        'convexbooster', 'aurabooster', 'vltoken',
    }
    _BRIDGE_PROTOCOLS = {
        'relayrouter', 'relayreceiver', 'relaydepository', 'relaybridge',
        'rangodiamond', 'rangobridge',
        'mayanswift', 'mayanbridge',
        'debridgerouter', 'debridgegate',
        'lzendpoint', 'lzreceiver', 'endpoint', 'endpointv2',  # LayerZero
        'stargatebridge', 'stargatepool',
        'hopbridge', 'hopl2bridge',
        'connextdiamond',
        'acrossspokepool',
        'ccipbridge', 'cciptokenpool',         # Chainlink CCIP
    }
    # Oracle function names — detected even when contract names are unknown/unverified.
    # A contract that calls or delegates to these functions is oracle infrastructure.
    _ORACLE_FUNCTION_NAMES = {
        'storkpublickey', 'getstorkpublickey',   # Stork oracle
        'latestanswer', 'latestrounddata',        # Chainlink AggregatorV3
        'getprice', 'getlatestprice',             # generic oracle getters
        'updatepricefeed', 'updatepricefeeds',    # Pyth-style push oracle
        'parsepricefeedsupdates',                 # Pyth
        'verifyandattest', 'verifyandstore',      # Stork attestation
    }
    # Known swap selectors on pool contracts (state-changing, not reads like slot0)
    _SWAP_SELECTORS = {
        '0x128acb08',  # UniswapV3Pool / CL Pool swap(address,bool,int256,uint160,bytes)
        '0x022c0d9f',  # UniswapV2Pair swap(uint256,uint256,address,bytes)
    }

    named_contracts_in_traces: set[str] = set()
    direct_callees: set[str] = set()    # depth <= 1 (this contract's immediate calls)
    matched_routers: list[str] = []
    matched_dex_pools: list[str] = []
    calls_into_dex_pool_swap = False    # True only for actual swap calls, not reads like slot0
    matched_lending: list[str] = []
    matched_staking: list[str] = []
    matched_bridges: list[str] = []
    matched_oracle_fns: list[str] = []   # oracle function names detected in any call/delegate
    this_contract_delegates_to: str = ''   # named target if this contract itself delegates
    this_contract_raw_delegate = False     # True if this contract has a raw-addr delegate
    all_traces_empty = True
    traces_only_raw_opcodes = True       # True if traces exist but contain zero decoded function calls
    callers: list[str] = []             # who sent each sampled transaction

    for t in traces:
        caller = t.get('caller', '')
        if caller:
            callers.append(caller)
        calls = t.get('protocol_calls', [])
        if calls:
            all_traces_empty = False
            # Check if any call has a real named contract or decoded function (not raw hex / "Unverified")
            _meaningless = {'unverified', 'unknown', ''}
            for c in calls:
                fn = c.get('function') or ''
                cn = (c.get('contract') or '').strip()
                has_real_fn = fn and not fn.startswith('0x')
                has_real_cn = cn and cn.lower() not in _meaningless and not cn.startswith('0x')
                if has_real_fn or has_real_cn:
                    traces_only_raw_opcodes = False
                    break
        for c in calls:
            cname_raw = (c.get('contract') or '').lower().replace(' ', '')
            depth = c.get('depth', 99)
            ct = c.get('call_type', '')
            cname = c.get('contract', '')

            # All named contracts (for display)
            if cname and not cname.startswith('0x'):
                named_contracts_in_traces.add(cname)

            # Direct callees only (depth <= 1)
            if depth <= 1:
                direct_callees.add(cname_raw)
                if ct == 'DELEGATECALL':
                    if cname and not cname.startswith('0x'):
                        this_contract_delegates_to = cname
                    elif cname.startswith('0x') or not cname:
                        this_contract_raw_delegate = True
                elif ct in ('CALL', 'STATICCALL'):
                    if any(r in cname_raw for r in _OFFICIAL_ROUTERS):
                        matched_routers.append(cname or cname_raw)
                    if any(p in cname_raw for p in _POOL_CONTRACTS):
                        matched_dex_pools.append(cname or cname_raw)
                        fn_lower = (c.get('function') or '').lower()
                        sel = c.get('selector', '')
                        if 'swap' in fn_lower or sel in _SWAP_SELECTORS:
                            calls_into_dex_pool_swap = True
                    if any(l in cname_raw for l in _LENDING_CONTRACTS):
                        matched_lending.append(cname or cname_raw)
                    if any(s in cname_raw for s in _STAKING_CONTRACTS):
                        matched_staking.append(cname or cname_raw)
                    if any(b in cname_raw for b in _BRIDGE_PROTOCOLS):
                        matched_bridges.append(cname or cname_raw)

            # Oracle function detection — works even when contract name is unknown/unverified
            for c in calls:
                fn_lower = (c.get('function') or '').lower().replace(' ', '').replace('_', '')
                if fn_lower in _ORACLE_FUNCTION_NAMES:
                    matched_oracle_fns.append(c.get('function') or fn_lower)

    named_contracts_summary = ', '.join(sorted(n for n in named_contracts_in_traces if n and not n.startswith('0x'))[:20]) or 'none'

    # ── Dominant ERC20 tokens ─────────────────────────────────────────────────
    # Track named contracts that receive ERC20 function calls across traces.
    # Split into "common" (well-known infra tokens — low signal) vs "novel" (everything
    # else — high signal: hints at a specific protocol or new asset).
    _ERC20_FUNCTIONS = {'transfer', 'transferfrom', 'approve', 'balanceof', 'allowance'}
    _COMMON_TOKENS = {
        # Major stablecoins
        'usdc', 'usdt', 'dai', 'frax', 'lusd', 'musd', 'cusd', 'usdbc', 'usde',
        'usdb', 'eurc', 'pyusd', 'crvusd', 'susd', 'busd', 'fdusd', 'tusd', 'gusd',
        # Wrapped natives & LSTs
        'weth', 'wbtc', 'cbbtc', 'cbeth', 'wsteth', 'reth', 'weeth', 'wmatic', 'wbnb',
        'ezeth', 'rseth', 'lseth', 'meth', 'sweth', 'oseth', 'ankreth', 'sfrxeth',
        # DEX reward / infra tokens
        'aero', 'velo', 'op', 'arb', 'crv', 'cvx', 'ldo', 'bal',
        'cake', 'sushi', 'joe', 'gmx', 'grail', 'rdnt', 'pendle',
        # Other ubiquitous tokens
        'link', 'uni', 'snx', 'comp', 'mkr', 'aave', 'eigen', 'ena', 'ondo',
    }

    token_trace_counts: dict[str, int] = {}
    for t in traces:
        seen_this_trace: set[str] = set()
        for c in t.get('protocol_calls', []):
            fn = (c.get('function') or '').lower()
            cname = c.get('contract', '')
            if fn in _ERC20_FUNCTIONS and cname and not cname.startswith('0x'):
                seen_this_trace.add(cname)
        for tok in seen_this_trace:
            token_trace_counts[tok] = token_trace_counts.get(tok, 0) + 1

    trace_count = len([t for t in traces if t])
    threshold = max(1, trace_count // 2)
    dominant_all = sorted(
        [tok for tok, cnt in token_trace_counts.items() if cnt >= threshold],
        key=lambda t: -token_trace_counts[t],
    )
    novel_tokens   = [t for t in dominant_all if t.lower() not in _COMMON_TOKENS][:5]
    common_tokens  = [t for t in dominant_all if t.lower() in _COMMON_TOKENS][:4]

    # Augment with Blockscout token-transfer names (higher reliability than trace decoding).
    bs_token_display: list[str] = []
    if token_transfers:
        for tt in token_transfers:
            name = tt.get('token_name', '').strip()
            if not name:
                continue
            sym = tt.get('token_symbol', '').strip()
            tok_type = tt.get('token_type', '').strip()
            label_str = f"{name}({sym})" if sym else name
            if tok_type:
                label_str = f"{label_str} [{tok_type}]"
            bs_token_display.append(label_str)
            nl = name.lower()
            if nl not in _COMMON_TOKENS:
                if name not in novel_tokens:
                    novel_tokens.append(name)
            else:
                if name not in common_tokens:
                    common_tokens.append(name)
        novel_tokens = novel_tokens[:5]
        common_tokens = common_tokens[:4]

    novel_tokens_str  = ', '.join(novel_tokens)  if novel_tokens  else 'none'
    common_tokens_str = ', '.join(common_tokens) if common_tokens else 'none'
    bs_tokens_str = ', '.join(bs_token_display[:8]) if bs_token_display else 'none'

    # No meaningful on-chain identity: unverified + no Blockscout name.
    # GitHub presence just means the address appears in some repo — it gives no protocol name.
    no_meaningful_identity = (bs_name == 'unknown')

    # Caller diversity: who is sending transactions to this contract?
    unique_callers = len(set(callers))
    total_traces_with_caller = len(callers)
    caller_diversity_pct = round(unique_callers / total_traces_with_caller * 100) if total_traces_with_caller else None

    if total_traces_with_caller < 8:
        caller_sample_note = f"LOW SAMPLE ({total_traces_with_caller} txns sampled) — diversity % unreliable, weight other signals more heavily"
    elif total_traces_with_caller < 15:
        caller_sample_note = f"MEDIUM SAMPLE ({total_traces_with_caller} txns sampled) — moderate confidence in diversity %"
    else:
        caller_sample_note = f"GOOD SAMPLE ({total_traces_with_caller} txns sampled) — diversity % reliable"

    if total_traces_with_caller == 0:
        caller_pattern = "unknown (no caller data)"
        caller_diversity_label = "UNKNOWN"
    elif caller_diversity_pct <= 20:
        caller_pattern = f"SINGLE/FEW OPERATORS — {unique_callers} unique caller(s) in {total_traces_with_caller} txns ({caller_diversity_pct}% diversity) → private/automated"
        caller_diversity_label = "PRIVATE"
    elif caller_diversity_pct >= 80 and avg_daa >= public_daa_threshold:
        # High ratio AND avg_daa above chain-relative threshold → truly public-facing
        caller_pattern = f"DIVERSE CALLERS — {unique_callers} unique callers in {total_traces_with_caller} txns ({caller_diversity_pct}% diversity) → public-facing"
        caller_diversity_label = "PUBLIC"
    elif caller_diversity_pct >= 80 and avg_daa < public_daa_threshold:
        # High ratio but avg_daa below chain-relative threshold → small rotating operator set, not truly public
        caller_pattern = f"SMALL ROTATING OPERATORS — {unique_callers} unique callers in {total_traces_with_caller} txns ({caller_diversity_pct}% diversity) but avg_daa={avg_daa:.1f} < chain threshold {public_daa_threshold:.1f} → permissioned/keeper pattern"
        caller_diversity_label = "KEEPER"
    else:
        caller_pattern = f"MULTI-OPERATOR — {unique_callers} unique callers in {total_traces_with_caller} txns ({caller_diversity_pct}% diversity) → permissioned/keeper pattern"
        caller_diversity_label = "KEEPER"

    user_prompt = f"""## Contract Data
Address: {address}
Chain: {origin_key} ({chain_id_str})
Blockscout name: {bs_name}  [generic proxy names already stripped — "unknown" means no real name]
Source verified: {is_verified}
GitHub found: {has_repo}

## Activity ({day_range} days)
Transactions: {txcount:,} | Avg daily active addresses: {avg_daa:.0f} | DAA/tx ratio: {daa_ratio:.2f}%
Gas: {gas_eth:.4f} ETH | Avg gas per tx: {avg_gas_per_tx:.8f} ETH (rel_cost vs chain median tx: {rel_cost:.2f}x)
Tx success rate (sample of last 30): {"n/a" if success_rate is None else f"{success_rate:.1%}"}
DAA/tx signal: {"AUTOMATED (<5% DAA/tx)" if daa_ratio < 5 and txcount > 0 else "BROAD USER BASE (>5% DAA/tx)" if txcount > 0 else "NO METRICS"}
Success-rate signal: {"LOW SUCCESS (<50%) — aggressive automated (MEV/arbitrage tries many txns, most fail)" if success_rate is not None and success_rate < 0.50 else "VERY LOW SUCCESS (<30%) — near-certain MEV/arbitrage bot" if success_rate is not None and success_rate < 0.30 else "HIGH SUCCESS (>=50%) — normal user-facing or efficient bot" if success_rate is not None else "n/a (no sample)"}
Gas-per-tx signal: {"HIGH COMPLEXITY (rel_cost >5x chain avg) — heavy multi-hop or MEV" if rel_cost > 5 else "ELEVATED COMPLEXITY (rel_cost 2-5x chain avg) — moderately complex" if rel_cost > 2 else "NORMAL COMPLEXITY (rel_cost <=2x chain avg)"}

## Pre-computed Signals  [computed from depth-0/1 direct calls only — more precise than full trace view]
Named contracts seen anywhere in traces: {named_contracts_summary}
Novel ERC20 tokens (≥50% of traces, NOT common infra): {novel_tokens_str}  [HIGH SIGNAL — unknown token = likely a specific new protocol]
Common ERC20 tokens (≥50% of traces, well-known infra): {common_tokens_str}  [low signal — ubiquitous reward/stable/wrapped tokens]
Blockscout token transfers (tokens flowing through this contract, direct API — more reliable than trace): {bs_tokens_str}  [HIGH SIGNAL if novel names present]
Direct calls into official DEX routers [depth≤1] (empty=none): {sorted(set(matched_routers))}
Direct calls into AMM pools [depth≤1] (empty=none): {sorted(set(matched_dex_pools))}
Direct calls into AMM pool swap functions [depth≤1, state-changing]: {calls_into_dex_pool_swap}
Direct calls into lending protocols [depth≤1] (Aave, Compound, Morpho, etc.) (empty=none): {sorted(set(matched_lending))}
Direct calls into staking/gauge contracts [depth≤1] (Gauge, Voter, MasterChef, etc.) (empty=none): {sorted(set(matched_staking))}
Direct calls into bridge protocols [depth≤1] (Relay, Rango, LayerZero, Stargate, etc.) (empty=none): {sorted(set(matched_bridges))}
Oracle function calls detected (any depth, works even when unverified) (empty=none): {sorted(set(matched_oracle_fns))}
This contract itself delegates to (named): {this_contract_delegates_to or "none"}
This contract has raw-address DELEGATE (EIP-1167 clone): {this_contract_raw_delegate}
All trace call graphs empty: {all_traces_empty}
Traces contain only raw opcodes (no decoded function/contract names): {traces_only_raw_opcodes}
No meaningful on-chain identity (unverified + no name + no GitHub): {no_meaningful_identity}
Caller pattern (who sent txns to this contract): {caller_pattern}
Caller diversity: {f"{caller_diversity_pct}%" if caller_diversity_pct is not None else "unknown"} unique callers / sampled txns  [PRIVATE ≤20% | KEEPER 20–80% | PUBLIC ≥80% AND avg_daa≥threshold]
Caller diversity sample quality: {caller_sample_note}
Caller diversity label: {caller_diversity_label}

## Emitted Event Signals  [decoded from contract's own logs]
Top events: {log_signals['summary']}
Has AccessControl (RoleGranted/Revoked/OwnershipTransferred): {log_signals['has_access_control']}
Has token Transfer/Approval events: {log_signals['has_token_transfers']}
Has DEX Swap events: {log_signals['has_swaps']}
Has bridge events (MessagePassed/SentMessage/TransactionDeposited): {log_signals['has_bridge_events']}
Has ERC-4337 events (UserOperationEvent/AccountDeployed): {log_signals['has_erc4337']}
Has staking events: {log_signals['has_staking']}
Has governance events (ProposalCreated/VoteCast): {log_signals['has_governance']}
Category hints from events: {', '.join(log_signals['category_hints']) or 'none'}

## Transaction Traces
{trace_section}

## Select usage_category (pick one of these IDs):
{categories['context']}"""

    response_schema = genai_types.Schema(
        type=genai_types.Type.OBJECT,
        properties={
            "contract_name": genai_types.Schema(
                type=genai_types.Type.STRING,
                description="Short descriptive name for the contract (max 50 chars)",
            ),
            "usage_category": genai_types.Schema(
                type=genai_types.Type.STRING,
                description="OLI usage category ID — must be one of the listed category IDs",
            ),
            "confidence": genai_types.Schema(
                type=genai_types.Type.NUMBER,
                description="Confidence score 0.0–1.0",
            ),
            "reasoning": genai_types.Schema(
                type=genai_types.Type.STRING,
                description="Required format: 'PATH X[subpath]: signal=val, signal=val → category. Ruled out Y because Z.' Always name the path taken. Always state what was ruled out. No filler, fragments OK.",
            ),
        },
        required=["contract_name", "usage_category"],
    )

    try:
        client = _get_gemini_client()

        def _call_gemini():
            return client.models.generate_content(
                model=GEMINI_MODEL,
                contents=user_prompt,
                config=genai_types.GenerateContentConfig(
                    system_instruction=_SYSTEM_INSTRUCTION,
                    response_mime_type="application/json",
                    response_schema=response_schema,
                    max_output_tokens=2048,
                    temperature=0.1,
                    thinking_config=genai_types.ThinkingConfig(thinking_budget=0),
                ),
            )

        logger.info(f"[Prompt] {address}\n{'-'*60}\n{user_prompt}\n{'-'*60}")

        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, _call_gemini)

        raw = response.text
        logger.info(f"[Response] {address}\n{'-'*60}\n{raw}\n{'-'*60}")

        if raw:
            # Gemini sometimes emits trailing commas — strip them before parsing
            cleaned = re.sub(r',\s*([}\]])', r'\1', raw)
            args = None
            try:
                args = json.loads(cleaned)
            except json.JSONDecodeError:
                # Gemini sometimes emits literal control chars inside string values
                repaired = re.sub(r'[\x00-\x09\x0b-\x1f\x7f]', ' ', cleaned)
                try:
                    args = json.loads(repaired)
                except json.JSONDecodeError:
                    # Last resort: regex extraction (handles truncation / unescaped quotes)
                    logger.warning(f"[Classifier] JSON repair fallback for {address}: {raw[:120]!r}")
                    args = _extract_json_fields(repaired)
            if args:
                if args.get('usage_category') not in valid_ids:
                    args['usage_category'] = fallback_id
                args.setdefault('confidence', 0.5)
                args.setdefault('reasoning', '')
                # Attach novel_tokens so sentinel can use them without re-computing
                args['novel_tokens'] = novel_tokens
                # TODO(contract_label_features): extend return dict with pre-computed signals so
                # automated_labeler.py can write them to contract_label_features without re-computing.
                # Add to args before return:
                #   args['common_tokens']              = common_tokens
                #   args['matched_routers']            = list(matched_routers)
                #   args['matched_dex_pools']          = list(matched_dex_pools)
                #   args['calls_into_dex_pool_swap']   = calls_into_dex_pool_swap
                #   args['matched_lending']            = list(matched_lending)
                #   args['matched_staking']            = list(matched_staking)
                #   args['matched_bridges']            = list(matched_bridges)
                #   args['matched_oracle_fns']         = list(matched_oracle_fns)
                #   args['delegates_to']               = this_contract_delegates_to or None
                #   args['raw_delegate']               = this_contract_raw_delegate
                #   args['all_traces_empty']           = all_traces_empty
                #   args['traces_only_raw_opcodes']    = traces_only_raw_opcodes
                #   args['named_contracts_in_traces']  = sorted(named_contracts_in_traces)
                #   args['caller_diversity_pct']       = caller_diversity_pct
                #   args['caller_diversity_label']     = caller_diversity_label
                #   args['unique_callers']             = unique_callers
                #   args['total_traces_sampled']       = total_traces_with_caller
                #   args['log_signals']                = log_signals   (full decode_log_signals dict)
                #   args['sentinel_fired']             = False          (True in sentinel fast-path)
                # Sentinel fast-path (see lines ~1060–1090) also needs sentinel_fired=True added.
                # Fallback return (see lines ~1006–1012) needs all fields with safe empty defaults.
                return args
            logger.warning(f"[Classifier] Could not parse Gemini response for {address}: {raw[:120]!r}")

        logger.warning(
            f"[Classifier] Empty Gemini response for {address}. "
            f"finish_reason={response.candidates[0].finish_reason if response.candidates else 'no candidates'}"
        )

    except Exception as e:
        logger.error(f"[Classifier] Gemini API error for {address}: {e}")

    # TODO(contract_label_features): add all pre-computed signal keys with safe empty defaults
    # to this fallback return so contract_label_features always gets a complete row even on failure.
    return {
        'contract_name': bs_name if bs_name != 'unknown' else f"Contract-{address[:8]}",
        'usage_category': fallback_id,
        'confidence': 0.0,
        'reasoning': 'Classification failed — using fallback.',
        'novel_tokens': novel_tokens,
    }


# ─── Batch Classifier ─────────────────────────────────────────────────────────

async def classify_contracts_batch(
    contracts: list[dict],
    concurrency: int = 3,
) -> list[dict]:
    """Classify a list of enriched contract dicts concurrently."""
    ssl_ctx = _ssl_ctx()
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)

    async with aiohttp.ClientSession(connector=connector) as session:
        await _oli_cache.get(session)

        sem = asyncio.Semaphore(concurrency)

        async def _classify_one(contract: dict) -> dict:
            async with sem:
                label = await classify_contract(
                    address=contract['address'],
                    origin_key=contract['origin_key'],
                    metrics=contract.get('metrics', {}),
                    blockscout=contract.get('blockscout', {}),
                    github=contract.get('github', {}),
                    traces=contract.get('traces', []),
                    session=session,
                )
                contract['label'] = label
                return contract

        tasks = [_classify_one(c) for c in contracts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    output = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            logger.error(f"[Classifier] Batch error for contract {i}: {r}")
            contracts[i]['label'] = {
                'contract_name': f"Contract-{contracts[i]['address'][:8]}",
                'usage_category': 'other',
                'confidence': 0.0,
                'reasoning': str(r),
            }
            output.append(contracts[i])
        else:
            output.append(r)

    return output
