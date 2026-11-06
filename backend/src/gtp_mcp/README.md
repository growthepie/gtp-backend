# growthepie MCP server

Read-only MCP tools over the public JSON API at `api.growthepie.com`, with usage metering.
It needs no database credentials and adds no load to Postgres.

| Tool | Answers |
|------|---------|
| `list_chains`, `list_metrics` | What is covered, and valid identifiers |
| `get_chain_metric` | Time series for one metric on one chain |
| `compare_chains` | All chains ranked by a metric on one day |
| `get_chain_overview` | Rankings, KPIs, blockspace, top apps, events |
| `get_current_fees` | Latest hourly transaction costs and throughput |
| `search_apps`, `top_apps` | Find apps, rank apps |
| `get_app`, `get_app_timeseries` | App metrics by chain, contracts, time series |
| `get_eth_overview`, `get_eth_timeseries` | ETH price, supply, burn, staking, gas paid, net supply growth, ETH/BTC |
| `get_eth_yields`, `get_eth_supply_flow` | Yield comparison, stake/lend/borrow, issuance vs. burn |
| `get_eth_market_comparison`, `get_eth_purchasing_power` | ETH against other assets and everyday prices |
| `get_eth_today` | AI-written daily highlight cards, which favour positive stories |
| `get_eth_exported` | ETH bridged to other chains |
| `get_live_chain_stats` | Live TPS, block time, and latest-block costs from `sse.growthepie.com` |
| `get_l2_economics`, `get_l2_economics_timeseries` | L2 revenue, costs, profit, and margin |
| `get_top_contracts` | Top 100 contracts per chain, on chains that publish them |
| `list_quick_bites`, `get_quick_bite` | Topic datasets: stablecoins, tokenized stocks, EIP-8004, Fusaka and more |

Files are several MB, so tools return slices. Upstream responses are cached in memory for 5 minutes,
and stale data is served if the API is unreachable. Live stats are cached for 10 seconds.

Quick bites go through a catalog in `gtp_quick_bites.py`, so a new published file needs a catalog entry. EIP-8004 agent
names, descriptions, and endpoints are dropped because they are unverified onchain text.

## Metering

Every MCP request is written to SQLite (`MCP_USAGE_DB`) by a background thread, so metering can't
delay or fail a tool call. If the queue fills up or the file can't be written, events are dropped.

Each row holds the time, method, tool, arguments, key name, IP hash, client name, user agent, status,
latency and response size. Raw IPs and raw keys are never stored. Arguments are stored as sent
(cut to 500 characters), which includes free-text `search_apps` queries.

`status` is `ok`, `tool_error`, `error`, `rate_limited` or `invalid_key`.

Attribution has two levels:

- **Anonymous:** counted by salted IP hash. A shared IP counts as one caller, and one person on two
  networks counts as two.
- **With a key:** counted by key name, with a higher rate limit. Clients send `Authorization: Bearer <key>`
  or `x-api-key`. An invalid key is rejected rather than treated as anonymous.

Client name comes from the MCP `initialize` call. Stateless tool calls usually carry only the user agent.

```bash
docker exec gtp-mcp python usage_report.py --days 30
docker exec gtp-mcp python usage_report.py --days 30 --json
```

## Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `MCP_ALLOWED_HOSTS` | `127.0.0.1:*,localhost:*` | Accepted `Host` headers. **Must include the public hostname** |
| `MCP_IP_SALT` | random per start | Set it, or IP hashes change on every restart |
| `MCP_USAGE_DB` | `/data/usage.sqlite` | Usage database |
| `MCP_API_KEYS_JSON` | empty | `{"name": "prefix:hash"}` |
| `MCP_KEY_PEPPER` | empty | Required when keys are configured |
| `MCP_RATE_ANON_PER_MINUTE` | `60` | Tool calls per IP |
| `MCP_RATE_KEY_PER_MINUTE` | `600` | Tool calls per key |
| `MCP_TRUST_PROXY` | `1` | Read the client IP from `X-Forwarded-For`. Set to `0` if not behind Caddy |
| `GTP_CACHE_TTL_SECONDS` | `300` | Upstream cache lifetime |
| `GTP_LIVE_CACHE_TTL_SECONDS` | `10` | Live stats cache lifetime |

Create a key with `MCP_KEY_PEPPER=... python usage_report.py --new-key alice`, add the printed entry to
`MCP_API_KEYS_JSON`, and restart.

## Deploy

```bash
cd backend/src/gtp_mcp
docker build -t gtp-mcp .
sudo mkdir -p /var/lib/gtp-mcp && sudo chown 10001 /var/lib/gtp-mcp
docker run -d --name gtp-mcp --restart unless-stopped --memory 1g \
  -p 127.0.0.1:8082:8080 -v /var/lib/gtp-mcp:/data \
  -e MCP_ALLOWED_HOSTS=mcp.growthepie.com \
  -e MCP_IP_SALT="$(openssl rand -hex 32)" \
  gtp-mcp
```

Keep the generated `MCP_IP_SALT`, for example in an env file; a new value on each `docker run` splits callers.

Add to `/etc/caddy/Caddyfile`, point DNS at this box, and run `sudo systemctl reload caddy`:

```
mcp.growthepie.com {
	reverse_proxy 127.0.0.1:8082
}
```

Connect a client:

```bash
claude mcp add --transport http growthepie https://mcp.growthepie.com/mcp
```

## Tests

```bash
pip install -r backend/src/gtp_mcp/requirements.txt
python -m unittest discover -s backend/tests -p test_gtp_mcp.py
```

These need their own virtualenv, because the pinned `mcp` and `starlette` differ from the Airflow environment.
