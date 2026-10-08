---
name: rpc-discovery
description: Use when finding new public RPCs for a chain, auditing sys_rpc_config for broken endpoints, or when a raw_* DAG fails because no good RPCs are available. Covers chainlist + chain docs as sources, the rpc_probe.py test script, decision rules, and SQL output.
---

# RPC Discovery & Audit

Goal: keep every PROD chain at **≥2 healthy public RPCs** in `sys_rpc_config`, by deactivating broken rows and adding good new ones.

## How RPCs are used (know this before changing rows)

- Raw DAGs read `get_chain_config()` (`src/adapters/rpc_funcs/utils.py`): `active = TRUE AND synced = TRUE` (celestia/starknet: `active` only).
- `utility_rpc_sync_check` (hourly, :35) sets `synced` for every **active** row: block height within 30 blocks of the best node (100 for arbitrum) **and** can serve a full block with transactions.
- So: `active` is ours to manage; `synced` is the sync check's. New rows go in `active=true, synced=false` and start being used after the next sync check.
- `url` is the **global primary key**: the same URL can't exist for two chains.
- The raw adapter needs `eth_getBlockByNumber(n, true)` and ideally `eth_getBlockReceipts`. Without block receipts it falls back to one `eth_getTransactionReceipt` per tx, which works but is slow.

## Sources for candidates

1. **chainlist**: `https://chainlist.org/rpcs.json`, matched by `evm_chain_id` from `main_config`. The script fetches it automatically.
2. **Chain docs**: the chain configs have no docs link, so search the web for `"<chain name> docs RPC endpoints"` / `"<chain name> network information"`. Only take URLs from the chain's **official docs domain** (or its official GitHub). Only public endpoints, never URLs containing an API key or token. Put them into a file, one `origin_key url` per line, and pass it as `--extra-urls`.

## Run the probe (read-only)

From `backend/`:

```bash
python -m src.adapters.rpc_funcs.rpc_probe --chains base,optimism --extra-urls docs_urls.txt --out <scratchpad>/rpc_audit
python -m src.adapters.rpc_funcs.rpc_probe --all-prod --out <scratchpad>/rpc_audit    # all PROD chains, ~30-40 min
```

Tests per URL: chainId, lag vs the best node (≤ max(30 blocks, 15 s)), full block with txs, `eth_getBlockReceipts` count, a ~30 day old block, and a 30-request `eth_blockNumber` burst. Anything that would change state (deactivate or add) is re-tested after `--retest-after` seconds (default 180); results that flip are marked `flaky` and left alone.

Outputs in `--out`:
- `sys_rpc_config_backup.csv`: exact table snapshot for restore. It contains private keys, so don't print or share it.
- `results.json`: per-URL results (keyed URLs masked)
- `proposed_update.sql`: one transaction, `UPDATE ... SET active=false, synced=false` with reasons and `-- expect N rows`, plus guarded `INSERT ... WHERE NOT EXISTS`

Non-EVM chains (celestia, starknet) are skipped; check those manually.

## Decision rules (built into the script; apply them when reviewing too)

- **Deactivate** only active rows that are broken on both passes: wrong chainId, no block number, too far behind, no full blocks, or ≥20/30 burst failures (or >5/30 with block receipts otherwise fine).
- **Never deactivate** rows with `special_use`, `realtime_use`, or a private/paid comment (`ms@...`, `personal`, `github`...). The script lists them as `protected`; report them to the user.
- **Missing block receipts only** means usable but slow. Keep the row active, and list new ones as `optional`, not added.
- **Reactivate** inactive rows that pass everything again (≤2/30 burst failures) before adding new URLs from the same provider. `realtime_use` rows may be reactivated: the realtime adapter (`get_realtime_rpcs_for_chain`) only reads active rows, so an inactive realtime row is unused. Call out that it becomes the chain's preferred realtime RPC.
- **Add** only candidates that pass everything including the 30-day block, with ≤2/30 burst failures, at most one per provider per chain, and none from a provider that already has a healthy row on that chain.
- **Never probe or print keyed URLs** (Alchemy `/v2/<key>`, QuickNode, conduit keys, `apikey=`...). The script masks them.
- **At risk**: any chain with <2 healthy after the change must be called out explicitly.

## Hard rules

- **Never write to `sys_rpc_config` yourself.** It's shared prod config, and the auto-mode classifier blocks it anyway. Hand the SQL file to the user, show it on request, and let them run it.
- Before handing over, sanity-check the SQL against the live table with SELECTs only: the UPDATE matches the expected row count, no protected rows are included, and no INSERT collides with an existing URL.

## Gotchas

- **Probes run from the Airflow host**, so production traffic shares the IP. Some 429s (drpc free tier, 1rpc, onfinality, nodeflare, meowrpc) are partly our own load. They still count, because they're unusable from prod.
- **drpc free tier flips** between passing and failing within minutes, so the re-test matters.
- **Usually broken**: `1rpc.io` (usage limit), `rpc.ankr.com/<chain>` (needs key now), `*-pokt.nodies.app` (no full blocks on free plan), `blockpi` public (521/402), `lava.build` (410).
- **Usually good**: `*.api.pocket.network`, `*.rpc.sentio.xyz`, `*-rpc.publicnode.com`, `*.gateway.tenderly.co`, blockmachine.
- **A raw DAG hanging 45 min isn't always a missing-RPC problem.** It used to be an adapter bug, where a dead RPC was restarted forever; that's fixed in `adapter_raw_rpc.py`. Check the task log for `Detected unfinished tasks with no active workers` before blaming the RPC list.
