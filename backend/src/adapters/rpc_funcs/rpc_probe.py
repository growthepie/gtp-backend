"""Read-only RPC audit: tests sys_rpc_config rows plus chainlist/docs candidates, proposes SQL.

Usage (from backend/):
    python -m src.adapters.rpc_funcs.rpc_probe --chains arbitrum,base --out /tmp/rpc_audit
    python -m src.adapters.rpc_funcs.rpc_probe --all-prod --extra-urls docs_urls.txt --out /tmp/rpc_audit

--extra-urls: text file with one `origin_key url` pair per line (e.g. endpoints found in chain docs).

Never writes to the database. Outputs in --out: backup CSV, results.json, proposed_update.sql.
See .claude/skills/rpc-discovery.md for the workflow and decision rules.
"""

import argparse
import concurrent.futures as cf
import hashlib
import json
import re
import time
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests

CHAINLIST_URL = "https://chainlist.org/rpcs.json"
TIMEOUT = 15
MAX_THREADS = 20
BURST = 30
HEALTHY_MAX_BURST_FAIL = 5   # existing rows: tolerate some rate limiting
ADD_MAX_BURST_FAIL = 2       # new rows: stricter
BROKEN_MIN_BURST_FAIL = 20   # rate limited into uselessness

# URLs carrying someone's API key/token: never probe, never print in full.
KEY_PATTERNS = [
    r"alchemy\.com/v2/(?!demo)", r"alchemy\.com/starknet/", r"infura\.io", r"quiknode\.pro", r"[?&](api_?key|key|verify)=",
    r"rpc\.ankr\.com/[a-z_]+/[0-9a-f]{20,}", r"blockpi\.network/v1/rpc/(?!public)", r"conduit\.xyz/[A-Za-z0-9]{10,}",
    r"gateway\.tenderly\.co/[A-Za-z0-9]{10,}", r"chainstack\.com/[0-9a-f]{16,}", r"grove\.city/v1/",
    r"/[0-9a-f]{32,}", r"/[A-Za-z0-9_\-]{30,}", r"vk_demo",
]
# Rows we never deactivate automatically (paid/private endpoints are tracked in `comment`).
PRIVATE_COMMENT = re.compile(r"ms@|personal|private|paid|github", re.I)


def keyed(url):
    return any(re.search(p, url) for p in KEY_PATTERNS)


def mask(url):
    if not keyed(url):
        return url
    return f"{urlparse(url).scheme}://{urlparse(url).netloc}/<masked:{hashlib.sha1(url.encode()).hexdigest()[:6]}>"


def norm(url):
    return url.rstrip("/").lower()


def provider(url):
    return ".".join(urlparse(url).netloc.split(":")[0].split(".")[-2:])


def call(url, method, params, timeout=TIMEOUT):
    try:
        r = requests.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                          timeout=timeout, headers={"Content-Type": "application/json"})
    except requests.exceptions.Timeout:
        return None, "timeout"
    except requests.exceptions.ConnectionError:
        return None, "connection_error"
    except Exception as e:
        return None, type(e).__name__
    if r.status_code != 200:
        return None, f"HTTP{r.status_code}"
    try:
        j = r.json()
    except ValueError:
        return None, "non_json"
    if isinstance(j, list):
        j = j[0] if j else {}
    if j.get("error"):
        msg = j["error"].get("message", "") if isinstance(j["error"], dict) else str(j["error"])
        return None, "rpc_err: " + re.sub(r"[A-Za-z0-9_\-]{24,}", "<x>", msg)[:70]
    return (j["result"], None) if "result" in j else (None, "no_result")


def to_int(x):
    return int(x, 16) if isinstance(x, str) else x


def probe_chain(chain_id, urls):
    """Quick parallel pass for chainId/head (so lag is comparable), then deep checks per URL."""
    res = {u: {"url": u} for u in urls}

    def quick(u):
        cid, e1 = call(u, "eth_chainId", [], timeout=10)
        head, e2 = call(u, "eth_blockNumber", [], timeout=10)
        return u, to_int(cid) if cid else None, to_int(head) if head else None, e1 or e2

    with cf.ThreadPoolExecutor(MAX_THREADS) as ex:
        for u, cid, head, err in ex.map(quick, urls):
            res[u].update(chain_id=cid, head=head, error=err if head is None or cid is None else None)
    good = [r for r in res.values() if r["head"] and r["chain_id"] == chain_id]
    if not good:
        return list(res.values()), None
    max_head = max(r["head"] for r in good)

    # Block time from the freshest node, to locate a ~30 day old block.
    block_time = 2.0
    ref = max(good, key=lambda r: r["head"])["url"]
    b1, _ = call(ref, "eth_getBlockByNumber", [hex(max_head - 10), False])
    b0, _ = call(ref, "eth_getBlockByNumber", [hex(max_head - 10010), False])
    if b1 and b0 and to_int(b1["timestamp"]) > to_int(b0["timestamp"]):
        block_time = (to_int(b1["timestamp"]) - to_int(b0["timestamp"])) / 10000
    target, old = max_head - 200, max(1, max_head - int(30 * 86400 / block_time))
    lag_limit = max(30, int(15 / block_time))

    def deep(u):
        out = {}
        blk, e = call(u, "eth_getBlockByNumber", [hex(target), True])
        txs = blk.get("transactions") if blk else None
        out["full_block"] = "ok" if isinstance(txs, list) and (not txs or isinstance(txs[0], dict)) else (e or "hashes_only")
        rc, e = call(u, "eth_getBlockReceipts", [hex(target)], timeout=20)
        out["receipts"] = "ok" if isinstance(rc, list) and out["full_block"] == "ok" and len(rc) == len(txs) else (e or "mismatch")
        ob, e = call(u, "eth_getBlockByNumber", [hex(old), False])
        out["old_30d"] = "ok" if ob else (e or "null")
        out["burst_fail"] = sum(bool(call(u, "eth_blockNumber", [], timeout=10)[1]) for _ in range(BURST))
        return u, out

    todo = [r["url"] for r in good]
    with cf.ThreadPoolExecutor(MAX_THREADS) as ex:
        for u, out in ex.map(deep, todo):
            res[u].update(out, behind=max_head - res[u]["head"])
    for r in res.values():
        r["lag_ok"] = r.get("behind") is not None and r["behind"] <= lag_limit
    return list(res.values()), {"max_head": max_head, "block_time": block_time, "lag_limit": lag_limit}


def status(r, chain_id):
    """healthy | no_receipts | broken (with reason)."""
    if r.get("chain_id") != chain_id:
        return "broken", r.get("error") or f"wrong chainId {r.get('chain_id')}"
    if r.get("head") is None:
        return "broken", r.get("error") or "no block number"
    if not r.get("lag_ok"):
        return "broken", f"behind {r.get('behind')} blocks"
    if r.get("full_block") != "ok":
        return "broken", f"no full blocks ({r.get('full_block')})"
    if r.get("burst_fail", BURST) >= BROKEN_MIN_BURST_FAIL:
        return "broken", f"rate limited ({r['burst_fail']}/{BURST} burst failures)"
    if r.get("receipts") != "ok":
        return "no_receipts", r.get("receipts")
    if r["burst_fail"] > HEALTHY_MAX_BURST_FAIL:
        return "broken", f"rate limited ({r['burst_fail']}/{BURST} burst failures)"
    return "healthy", ""


def audit_chain(origin_key, chain_id, rows, candidates, retest_after):
    """rows: sys_rpc_config rows for the chain. candidates: [(url, source)] not in the table."""
    testable = [u for u in rows.url if not keyed(u)] + [u for u, _ in candidates]
    results, meta = probe_chain(chain_id, testable)
    by_url = {r["url"]: r for r in results}
    for r in results:
        r["status"], r["reason"] = status(r, chain_id)

    # Re-test everything that would change state, so one bad minute doesn't drop or add an RPC.
    changing = [u for u in rows[rows.active].url if u in by_url and by_url[u]["status"] == "broken"]
    changing += [u for u, _ in candidates if by_url[u]["status"] == "healthy"]
    changing += [u for u in rows[~rows.active].url if u in by_url and by_url[u]["status"] == "healthy"]
    if changing and retest_after:
        time.sleep(retest_after)
        # include a few healthy rows as reference so lag is measured against the real chain head
        refs = [u for u in rows.url if u in by_url and by_url[u]["status"] == "healthy" and u not in changing][:3]
        for r in probe_chain(chain_id, changing + refs)[0]:
            if r["url"] in refs:
                continue
            s, reason = status(r, chain_id)
            first = by_url[r["url"]]
            if s != first["status"]:
                first["status"], first["reason"] = "flaky", f"{first['status']} then {s} {reason}".strip()

    deactivate, protected, adds, optional = [], [], [], []
    for row in rows.itertuples():
        r = by_url.get(row.url)
        if not row.active or r is None or r["status"] != "broken":
            continue
        if row.special_use or row.realtime_use or PRIVATE_COMMENT.search(str(row.comment or "")):
            protected.append((row.url, r["reason"]))
        else:
            deactivate.append((row.url, r["reason"]))
    healthy_now = [u for u in rows[rows.active].url if u in by_url and by_url[u]["status"] == "healthy"]
    providers = {provider(u) for u in healthy_now}
    # Inactive rows that work again: reactivate before adding new URLs from the same provider.
    reactivate = []
    for row in rows[~rows.active].itertuples():
        r = by_url.get(row.url)
        if r and r["status"] == "healthy" and r["burst_fail"] <= ADD_MAX_BURST_FAIL and provider(row.url) not in providers:
            providers.add(provider(row.url))
            reactivate.append((row.url, "realtime_use row" if row.realtime_use else ""))
    for u, src in candidates:
        r = by_url[u]
        if r["status"] == "healthy" and r["old_30d"] == "ok" and r["burst_fail"] <= ADD_MAX_BURST_FAIL:
            if provider(u) not in providers:
                providers.add(provider(u))
                adds.append((u, src))
        elif r["status"] == "no_receipts":
            optional.append((u, "no eth_getBlockReceipts"))
    return {"origin_key": origin_key, "meta": meta, "results": results, "healthy_now": healthy_now,
            "deactivate": deactivate, "protected": protected, "reactivate": reactivate, "add": adds, "optional": optional,
            "keyed_untested": [mask(u) for u in rows.url if keyed(u)]}


def chainlist_candidates(chainlist, chain_id, existing):
    out, seen = [], set()
    for chain in chainlist:
        if chain.get("chainId") != chain_id:
            continue
        for rpc in chain.get("rpc", []):
            u = (rpc if isinstance(rpc, str) else rpc.get("url", "")).strip()
            if not u.startswith("https://") or "{" in u or keyed(u) or norm(u) in existing or norm(u) in seen:
                continue
            seen.add(norm(u))
            out.append((u, "chainlist"))
    return out


def healthy_after(a):
    return len(a["healthy_now"]) + len(a["reactivate"]) + len(a["add"])


def sql_literal(s):
    return "'" + s.replace("'", "''") + "'"


def write_sql(audits, path):
    today = date.today().isoformat()
    lines = [f"-- Proposed sys_rpc_config changes ({today}), generated by rpc_probe.py. Review before running.", "BEGIN;", ""]
    for a in audits:
        if not (a["deactivate"] or a["add"] or a["reactivate"]):
            continue
        lines.append(f"-- {a['origin_key']}: healthy now {len(a['healthy_now'])}, after {healthy_after(a)}")
        if a["deactivate"]:
            lines.append("UPDATE sys_rpc_config SET active = false, synced = false")
            lines.append(f"WHERE origin_key = {sql_literal(a['origin_key'])} AND url IN (")
            for i, (u, reason) in enumerate(a["deactivate"]):
                lines.append(f"    {sql_literal(u)}{',' if i < len(a['deactivate']) - 1 else ''}  -- {reason}")
            lines.append(f");  -- expect {len(a['deactivate'])} rows")
        if a["reactivate"]:
            lines.append("UPDATE sys_rpc_config SET active = true")
            lines.append(f"WHERE origin_key = {sql_literal(a['origin_key'])} AND active = false AND url IN (")
            for i, (u, note) in enumerate(a["reactivate"]):
                lines.append(f"    {sql_literal(u)}{',' if i < len(a['reactivate']) - 1 else ''}  -- passes all checks again {note}".rstrip())
            lines.append(f");  -- expect {len(a['reactivate'])} rows")
        for u, src in a["add"]:
            # url is a global primary key, so guard against it existing under another chain
            lines.append("INSERT INTO sys_rpc_config (url, origin_key, active, workers, synced, special_use, realtime_use, comment)")
            lines.append(f"SELECT {sql_literal(u)}, {sql_literal(a['origin_key'])}, true, 1, false, false, false, "
                         f"{sql_literal(f'{src} {today}')}")
            lines.append(f"WHERE NOT EXISTS (SELECT 1 FROM sys_rpc_config WHERE url = {sql_literal(u)});")
        lines.append("")
    lines.append("COMMIT;")
    Path(path).write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--chains", help="comma-separated origin_keys")
    target.add_argument("--all-prod", action="store_true", help="all chains with api_deployment_flag == PROD")
    parser.add_argument("--extra-urls", help="file with `origin_key url` lines (e.g. from chain docs)")
    parser.add_argument("--retest-after", type=int, default=180, help="seconds before re-testing state changes (0 = off)")
    parser.add_argument("--out", required=True, help="output directory")
    args = parser.parse_args()

    from src.db_connector import DbConnector
    from src.main_config import get_main_config

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    table = pd.read_sql("SELECT * FROM sys_rpc_config ORDER BY origin_key, url", DbConnector().engine)
    table.to_csv(out / "sys_rpc_config_backup.csv", index=False)
    for col in ("active", "special_use", "realtime_use"):
        table[col] = table[col].fillna(False).astype(bool)

    configs = {c.origin_key: c for c in get_main_config()}
    if args.all_prod:
        chains = [k for k, c in configs.items() if c.api_deployment_flag == "PROD" and k in set(table.origin_key)]
    else:
        chains = [c.strip() for c in args.chains.split(",")]

    extra = {}
    if args.extra_urls:
        for line in Path(args.extra_urls).read_text().splitlines():
            if line.strip() and not line.startswith("#"):
                key, url = line.split()[:2]
                extra.setdefault(key, []).append((url, "docs"))

    chainlist = requests.get(CHAINLIST_URL, timeout=60).json()
    existing = {norm(u) for u in table.url}
    audits = []
    for key in chains:
        chain_id = getattr(configs.get(key), "evm_chain_id", None)
        if not chain_id:
            print(f"{key}: no EVM chain id in main_config, skipped (check non-EVM chains manually)")
            continue
        rows = table[table.origin_key == key]
        candidates = [(u, s) for u, s in extra.get(key, []) if norm(u) not in existing and not keyed(u)]
        candidates += [c for c in chainlist_candidates(chainlist, chain_id, existing) if norm(c[0]) not in {norm(u) for u, _ in candidates}]
        a = audit_chain(key, chain_id, rows, candidates, args.retest_after)
        audits.append(a)
        after = healthy_after(a)
        print(f"{key}: healthy {len(a['healthy_now'])} | deactivate {len(a['deactivate'])} | "
              f"reactivate {len(a['reactivate'])} | add {len(a['add'])} | "
              f"after {after}{'  <-- AT RISK' if after < 2 else ''}"
              f"{' | protected broken: ' + str(len(a['protected'])) if a['protected'] else ''}", flush=True)

    for a in audits:
        for r in a["results"]:
            r["url"] = mask(r["url"])
    (out / "results.json").write_text(json.dumps(audits, indent=1, default=str))
    write_sql(audits, out / "proposed_update.sql")
    print(f"Wrote {out}/proposed_update.sql, results.json, sys_rpc_config_backup.csv")


if __name__ == "__main__":
    main()
