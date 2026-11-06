"""growthepie MCP server: read-only tools over the public JSON API, with usage metering.

Run with `python server.py`. Endpoint: POST /mcp (Streamable HTTP, stateless).
See README.md for deployment, keys and usage reports.
"""

from __future__ import annotations

import atexit
import logging
import os
import secrets
from typing import Annotated, Literal, Optional

import uvicorn
from mcp.server import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from pydantic import Field
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse

from gtp_data import GtpData
from gtp_eth import EthData
from gtp_quick_bites import QuickBites
from metering import ApiKeys, MeteringMiddleware, RateLimiter, UsageMeter

logger = logging.getLogger("gtp_mcp")

INSTRUCTIONS = """growthepie publishes onchain metrics for Ethereum and its Layer 2s, app-level usage, and data on ETH the asset.
Start with list_chains or list_metrics to find valid identifiers, and search_apps to find an app's owner_project key.
For ETH as an asset, start with get_eth_overview. For topical datasets such as stablecoins, start with list_quick_bites.
change_ratio fields are fractions: 0.25 means +25%. Yields and shares are fractions too.
Attribute figures to growthepie.com, and name third-party sources when a result lists them."""

READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)

Limit = Annotated[int, Field(ge=1, le=730, description="Newest points to return")]
Unit = Annotated[Literal["usd", "eth"], Field(description="Ignored for count metrics such as txcount")]
DateArg = Annotated[Optional[str], Field(description="YYYY-MM-DD (UTC)")]


def build_server(data: GtpData, middleware: MeteringMiddleware) -> MCPServer:
    mcp = MCPServer(name="growthepie", version="1.0.0", instructions=INSTRUCTIONS,
                    website_url="https://www.growthepie.com", middleware=[middleware])
    eth = EthData(data.api)
    bites = QuickBites(data.api)

    @mcp.tool(annotations=READ_ONLY)
    async def list_chains() -> dict:
        """List the chains growthepie covers, with origin_key, stack, DA layer, and supported metrics."""
        return await data.list_chains()

    @mcp.tool(annotations=READ_ONLY)
    async def list_metrics() -> dict:
        """List chain-level and app-level metrics with their units and definitions."""
        return await data.list_metrics()

    @mcp.tool(annotations=READ_ONLY)
    async def get_chain_metric(
        chain: Annotated[str, Field(description="origin_key, e.g. arbitrum")],
        metric: Annotated[str, Field(description="Metric key from list_chains, e.g. txcount")],
        granularity: Literal["daily", "daily_7d_rolling", "weekly", "monthly", "quarterly", "hourly"] = "daily",
        unit: Unit = "usd",
        start_date: DateArg = None,
        end_date: DateArg = None,
        limit: Limit = 90,
    ) -> dict:
        """Get a time series for one metric on one chain, plus a recent summary and period-over-period change."""
        return await data.get_chain_metric(chain, metric, granularity, unit, start_date, end_date, limit)

    @mcp.tool(annotations=READ_ONLY)
    async def compare_chains(
        metric: Annotated[str, Field(description="Metric key from list_metrics, e.g. daa")],
        date: Annotated[Optional[str], Field(description="YYYY-MM-DD within the last 90 days; default latest complete day")] = None,
        limit: Annotated[int, Field(ge=1, le=50)] = 30,
    ) -> dict:
        """Rank all chains by a metric on one day. Monetary metrics are in USD."""
        return await data.compare_chains(metric, date, limit)

    @mcp.tool(annotations=READ_ONLY)
    async def get_chain_overview(chain: Annotated[str, Field(description="origin_key")]) -> dict:
        """Get a chain's rankings, current KPIs, blockspace use by category, top apps, and recent events."""
        return await data.get_chain_overview(chain)

    @mcp.tool(annotations=READ_ONLY)
    async def get_current_fees(chain: Annotated[Optional[str], Field(description="origin_key; omit for all")] = None) -> dict:
        """Get the latest hourly transaction costs and throughput, sorted cheapest first."""
        return await data.get_current_fees(chain)

    @mcp.tool(annotations=READ_ONLY)
    async def get_live_chain_stats(chain: Annotated[Optional[str], Field(description="Chain key; omit for all chains and global totals")] = None) -> dict:
        """Get live TPS, block time, and latest-block transaction costs, updated every few seconds."""
        return await data.get_live_chain_stats(chain)

    @mcp.tool(annotations=READ_ONLY)
    async def get_l2_economics(
        chain: Annotated[Optional[str], Field(description="origin_key; omit to rank all L2s by profit")] = None,
        period: Literal["1d", "7d", "30d", "90d", "180d", "365d", "max"] = "30d",
        unit: Literal["usd", "eth"] = "usd",
    ) -> dict:
        """Get L2 revenue, L1 and blob costs, profit, margin, and data posted for a period."""
        return await data.get_l2_economics(chain, period, unit)

    @mcp.tool(annotations=READ_ONLY)
    async def get_l2_economics_timeseries(
        metric: Literal["revenue", "costs", "profit"],
        chain: Annotated[Optional[str], Field(description="origin_key; omit for all L2s combined")] = None,
        granularity: Literal["daily", "monthly"] = "daily",
        unit: Literal["usd", "eth"] = "usd",
        start_date: DateArg = None,
        end_date: DateArg = None,
        limit: Limit = 90,
    ) -> dict:
        """Get L2 revenue, costs, or profit over time, for one chain or all L2s combined."""
        return await data.get_l2_economics_timeseries(metric, chain, granularity, unit, start_date, end_date, limit)

    @mcp.tool(annotations=READ_ONLY)
    async def get_top_contracts(
        chain: Annotated[str, Field(description="origin_key; not every chain publishes this")],
        sort_by: Literal["txcount", "gas_fees_usd"] = "txcount",
        limit: Annotated[int, Field(ge=1, le=100)] = 25,
    ) -> dict:
        """Get the top contracts on a chain over the last 180 days, with labels."""
        return await data.get_top_contracts(chain, sort_by, limit)

    @mcp.tool(annotations=READ_ONLY)
    async def list_quick_bites(topic: Annotated[Optional[str], Field(description="Omit to list topics; pass one to see its datasets and keys")] = None) -> dict:
        """List quick bite topics such as stablecoins, tokenized stocks, EIP-8004 agents, Fusaka, and Linea burn."""
        return await bites.list(topic)

    @mcp.tool(annotations=READ_ONLY)
    async def get_quick_bite(
        topic: Annotated[str, Field(description="Topic from list_quick_bites")],
        dataset: Annotated[str, Field(description="Dataset from list_quick_bites")],
        key: Annotated[Optional[str], Field(description="Required by some datasets, such as a chain, fiat currency, or ticker")] = None,
        limit: Annotated[int, Field(ge=1, le=365, description="Rows per table; newest kept for time series. Lowered automatically for large results")] = 30,
    ) -> dict:
        """Read one quick bite dataset. Call list_quick_bites first to find topics, datasets, and keys."""
        return await bites.get(topic, dataset, key, limit)

    @mcp.tool(annotations=READ_ONLY)
    async def search_apps(
        query: Annotated[str, Field(min_length=2, description="App name or keyword")],
        limit: Annotated[int, Field(ge=1, le=25)] = 10,
    ) -> dict:
        """Find apps by name or description and get their owner_project key."""
        return await data.search_apps(query, limit)

    @mcp.tool(annotations=READ_ONLY)
    async def top_apps(
        metric: Literal["txcount", "daa", "gas_fees_usd", "gas_fees_eth", "stables_balance_usd"] = "txcount",
        timeframe: Literal["1d", "7d", "30d", "90d", "365d", "max"] = "7d",
        chain: Annotated[Optional[str], Field(description="origin_key; omit for all chains")] = None,
        limit: Annotated[int, Field(ge=1, le=50)] = 20,
    ) -> dict:
        """Rank apps by usage over a timeframe, with change against the previous period."""
        return await data.top_apps(metric, timeframe, chain, limit)

    @mcp.tool(annotations=READ_ONLY)
    async def get_app(
        owner_project: Annotated[str, Field(description="Key from search_apps, e.g. aave")],
        period: Literal["1d", "7d", "30d", "90d", "180d", "365d", "max"] = "30d",
    ) -> dict:
        """Get an app's profile, metrics by chain for a period, and its top contracts."""
        return await data.get_app(owner_project, period)

    @mcp.tool(annotations=READ_ONLY)
    async def get_app_timeseries(
        owner_project: Annotated[str, Field(description="Key from search_apps")],
        metric: Annotated[str, Field(description="App metric key from list_metrics, e.g. gas_fees")],
        chain: Annotated[Optional[str], Field(description="origin_key; required for metrics that cannot be summed, such as daa")] = None,
        granularity: Literal["daily", "hourly"] = "daily",
        unit: Unit = "usd",
        start_date: DateArg = None,
        end_date: DateArg = None,
        limit: Limit = 90,
    ) -> dict:
        """Get a time series for one app metric, on one chain or summed across chains."""
        return await data.get_app_timeseries(owner_project, metric, chain, granularity, unit, start_date, end_date, limit)

    @mcp.tool(annotations=READ_ONLY)
    async def get_eth_overview() -> dict:
        """Get a snapshot of ETH the asset: price, supply, burn, staking, market cap, wallets, and collateral."""
        return await eth.get_overview()

    @mcp.tool(annotations=READ_ONLY)
    async def get_eth_timeseries(
        series: Annotated[str, Field(description="eth_price, eth_supply, eth_burned, staked_share, staking_yield (recent, use timeframe); or gas_paid, eth_btc_ratio, net_supply_growth, supply_history (daily, full history)")],
        timeframe: Annotated[Literal["24h", "7d", "60d", "1y", "5y"], Field(description="Sets the window and bucket size for the first group of series")] = "60d",
        unit: Annotated[Literal["usd", "eth"], Field(description="Only applies to gas_paid")] = "eth",
        start_date: DateArg = None,
        end_date: DateArg = None,
        limit: Limit = 90,
    ) -> dict:
        """Get a time series for ETH price, supply, burn, staking, gas paid, net supply growth, or ETH/BTC."""
        return await eth.get_timeseries(series, timeframe, unit, start_date, end_date, limit)

    @mcp.tool(annotations=READ_ONLY)
    async def get_eth_yields() -> dict:
        """Compare ETH staking yield with rates, dividends, and savings, and get Aave stake, lend, and borrow terms."""
        return await eth.get_yields()

    @mcp.tool(annotations=READ_ONLY)
    async def get_eth_supply_flow() -> dict:
        """Get weekly ETH issuance against burn, the break-even burn, and annual stock change against USD and gold."""
        return await eth.get_supply_flow()

    @mcp.tool(annotations=READ_ONLY)
    async def get_eth_market_comparison(
        category: Annotated[Optional[Literal["crypto", "equity", "commodity"]], Field(description="Omit for all")] = None,
        limit: Annotated[int, Field(ge=1, le=80)] = 20,
    ) -> dict:
        """Rank ETH's market cap against top crypto assets, companies, and commodities."""
        return await eth.get_market_comparison(category, limit)

    @mcp.tool(annotations=READ_ONLY)
    async def get_eth_purchasing_power(
        region: Annotated[Optional[Literal["us", "uk", "euro_area", "japan"]], Field(description="Omit for all")] = None,
    ) -> dict:
        """Get how many everyday goods one ETH buys today against a year ago, by region."""
        return await eth.get_purchasing_power(region)

    @mcp.tool(annotations=READ_ONLY)
    async def get_eth_today(limit: Annotated[int, Field(ge=1, le=30)] = 7) -> dict:
        """Get recent 'Today in ETH' cards. They are AI-written highlights that favour positive developments."""
        return await eth.get_today(limit)

    @mcp.tool(annotations=READ_ONLY)
    async def get_eth_exported(
        entity: Annotated[Optional[str], Field(description="Key such as total or base; omit to rank all entities")] = None,
        unit: Literal["usd", "eth"] = "eth",
        start_date: DateArg = None,
        end_date: DateArg = None,
        limit: Annotated[int, Field(ge=1, le=730, description="Entities to rank, or points to return for one entity")] = 40,
    ) -> dict:
        """Get ETH exported from Ethereum to other chains: all entities ranked, or a time series for one."""
        return await eth.get_exported(entity, unit, start_date, end_date, limit)

    @mcp.custom_route("/health", methods=["GET"])
    async def health(_: Request) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    return mcp


def create_app(data: Optional[GtpData] = None, meter: Optional[UsageMeter] = None,
               limiter: Optional[RateLimiter] = None) -> Starlette:
    meter = meter or UsageMeter(os.getenv("MCP_USAGE_DB", "/data/usage.sqlite"))
    atexit.register(meter.close)
    ip_salt = os.getenv("MCP_IP_SALT")
    if not ip_salt:
        logger.warning("MCP_IP_SALT is not set: IP hashes will change on every restart")
        ip_salt = secrets.token_hex(32)
    middleware = MeteringMiddleware(
        meter, ApiKeys.from_env(), limiter or RateLimiter(), ip_salt,
        anonymous_per_minute=int(os.getenv("MCP_RATE_ANON_PER_MINUTE", "60")),
        keyed_per_minute=int(os.getenv("MCP_RATE_KEY_PER_MINUTE", "600")),
        trust_proxy=os.getenv("MCP_TRUST_PROXY", "1") == "1",
    )
    hosts = [host.strip() for host in os.getenv("MCP_ALLOWED_HOSTS", "127.0.0.1:*,localhost:*").split(",") if host.strip()]
    return build_server(data or GtpData(), middleware).streamable_http_app(
        stateless_http=True,
        json_response=True,
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=True, allowed_hosts=hosts),
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(create_app(), host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8080")),
                proxy_headers=False)
