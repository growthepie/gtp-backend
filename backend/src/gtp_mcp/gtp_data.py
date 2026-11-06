"""Read-only access to the public growthepie JSON API, sliced to LLM-sized answers.

Everything comes from https://api.growthepie.com, so this service needs no
database credentials and adds no load to Postgres. Many files are several MB;
every method here returns a bounded slice, never a whole file. Chain, metric and
app identifiers are validated against the master/labels files before they are
used in a URL.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import OrderedDict
from datetime import date, datetime, timezone
from typing import Any, Optional

import httpx
from mcp.server.mcpserver.exceptions import ToolError

BASE_URL = os.getenv("GTP_API_BASE_URL", "https://api.growthepie.com/v1").rstrip("/")
LIVE_BASE_URL = os.getenv("GTP_LIVE_BASE_URL", "https://sse.growthepie.com").rstrip("/")
LIVE_CACHE_TTL_SECONDS = int(os.getenv("GTP_LIVE_CACHE_TTL_SECONDS", "10"))
LIVE_STALE_SECONDS = 300
CACHE_TTL_SECONDS = int(os.getenv("GTP_CACHE_TTL_SECONDS", "300"))
# apps/details files parse to tens of MB each, so this bound is what caps memory.
CACHE_MAX_ENTRIES = int(os.getenv("GTP_CACHE_MAX_ENTRIES", "32"))

SOURCE = "growthepie.com"
# Entries in master.json that are aggregates or buckets, not chains with their own files.
NON_CHAIN_KEYS = {"all_l2s", "multiple"}
GRANULARITIES = ("daily", "daily_7d_rolling", "weekly", "monthly", "quarterly", "hourly")
APP_PERIODS = ("1d", "7d", "30d", "90d", "180d", "365d", "max")
APP_OVERVIEW_TIMEFRAMES = ("1d", "7d", "30d", "90d", "365d", "max")
APP_RANKING_COLUMNS = ("txcount", "daa", "gas_fees_usd", "gas_fees_eth", "stables_balance_usd")
# Summing these across chains is meaningful. daa double-counts multichain users, and
# success_rate/prices are not additive at all.
APP_ADDITIVE_METRICS = {"txcount", "gas_fees", "token_volume", "stables_balance"}
# How last_7d/last_30d are computed, from each metric's monthly_agg in master.json.
SUMMARY_AGGREGATION = {"sum": "sum", "avg": "average", "maa": "unique_addresses"}
MAX_POINTS = 730
# A date counts as complete once this share of chains reported it; the newest date
# in fundamentals.json is often partial.
MIN_DATE_COVERAGE = 0.8


# ToolError is what lets these messages reach the model. Any other exception type
# is reported by the SDK as a bare "Error executing tool <name>".
LIVE_COST_FIELDS = {"median_tx_cost_usd": "tx_cost_median_usd", "avg_tx_cost_usd": "tx_cost_avg_usd",
                    "transfer_cost_usd": "tx_cost_erc20_transfer_usd", "swap_cost_usd": "tx_cost_swap_usd"}
LIVE_NOTE = ("Snapshot of the latest block, so costs swing from block to block; use get_current_fees for an hourly "
             "median. Null cost means the feed does not report one for this chain.")
ECONOMICS_NOTE = ("revenue is fees paid by users, costs are L1 settlement and blob costs, and profit is the difference. "
                  "Sequencer and infrastructure costs are not included. profit_margin is a fraction.")
TOP_CONTRACT_FIELDS = ("address", "name", "owner_project", "usage_category", "deployment_date", "txcount_180d",
                       "gas_fees_usd_180d")


class DataError(ToolError):
    """The caller's request cannot be answered."""


class UpstreamError(ToolError):
    """api.growthepie.com could not be reached and no cached copy exists."""


class ApiClient:
    """In-memory TTL + LRU cache over the JSON API. Serves stale data if upstream fails."""

    def __init__(self, base_url: str = BASE_URL, ttl: int = CACHE_TTL_SECONDS,
                 max_entries: int = CACHE_MAX_ENTRIES, transport: Optional[httpx.AsyncBaseTransport] = None):
        self.base_url = base_url
        self.ttl = ttl
        self.max_entries = max_entries
        self._cache: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._locks: dict[str, asyncio.Lock] = {}
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            headers={"User-Agent": "gtp-mcp/1.0"},
            transport=transport,
        )

    def _fresh(self, path: str) -> Optional[Any]:
        entry = self._cache.get(path)
        if entry and time.monotonic() - entry[0] < self.ttl:
            self._cache.move_to_end(path)
            return entry[1]
        return None

    async def get(self, path: str) -> Any:
        cached = self._fresh(path)
        if cached is not None:
            return cached
        # One fetch per path, so a burst after expiry does not stampede CloudFront.
        lock = self._locks.setdefault(path, asyncio.Lock())
        async with lock:
            cached = self._fresh(path)
            if cached is not None:
                return cached
            try:
                response = await self._client.get(f"{self.base_url}/{path}")
                response.raise_for_status()
                data = response.json()
            except (httpx.HTTPError, ValueError) as exc:
                stale = self._cache.get(path)
                if stale:
                    return stale[1]
                # CloudFront answers 403 for objects that do not exist.
                if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in (403, 404):
                    raise DataError(f"growthepie publishes no data at {path}") from exc
                raise UpstreamError(f"growthepie API unavailable for {path}") from exc
            self._cache[path] = (time.monotonic(), data)
            self._cache.move_to_end(path)
            while len(self._cache) > self.max_entries:
                self._cache.popitem(last=False)
            return data

    async def aclose(self) -> None:
        await self._client.aclose()


def normalize_unix(value: float) -> float:
    """Return unix milliseconds. Per-chain monthly rows in economics.json are in microseconds."""
    return value / 1000 if value > 1e14 else value


def to_date(unix_ms: float) -> str:
    return datetime.fromtimestamp(unix_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def to_hour(unix_ms: float) -> str:
    return datetime.fromtimestamp(unix_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:00Z")


def rows(table: Optional[dict]) -> list[dict]:
    """Turn the API's {types: [...], data: [[...]]} layout into a list of dicts."""
    if not table:
        return []
    types = table.get("types", [])
    return [dict(zip(types, row)) for row in table.get("data", [])]


def single_row(table: Optional[dict]) -> dict:
    """Turn the API's flat {types: [...], data: [...]} layout into one dict."""
    return dict(zip(table.get("types", []), table.get("data", []))) if table else {}


def parse_date(value: Optional[str], name: str) -> Optional[date]:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError):
        raise DataError(f"{name} must be a YYYY-MM-DD date, got {value!r}") from None


def clamp(value: int, low: int, high: int) -> int:
    return max(low, min(int(value), high))


def choose(value: str, options: tuple[str, ...] | list[str] | set[str], name: str) -> str:
    if value not in options:
        raise DataError(f"Unknown {name} {value!r}. Valid values: {', '.join(sorted(options))}")
    return value


def value_column(types: list[str], unit: str) -> str:
    """Pick the column for a unit. Count metrics have a single 'value' column."""
    if "value" in types:
        return "value"
    choose(unit, ("usd", "eth"), "unit")
    if unit not in types:
        raise DataError(f"Unit {unit!r} is not available, this series has: {', '.join(t for t in types if t != 'unix')}")
    return unit


def slice_series(points: list[list], types: list[str], column: str, start: Optional[str], end: Optional[str],
                 limit: int, hourly: bool) -> dict:
    """Date-filter a [unix, ...] series and keep the newest `limit` points, oldest first."""
    start_day, end_day = parse_date(start, "start_date"), parse_date(end, "end_date")
    if start_day and end_day and start_day > end_day:
        raise DataError("start_date is after end_date")
    unix_i, value_i = types.index("unix"), types.index(column)
    fmt = to_hour if hourly else to_date
    selected = []
    for point in sorted(points, key=lambda p: p[unix_i]):
        day = date.fromisoformat(to_date(point[unix_i]))
        if (start_day and day < start_day) or (end_day and day > end_day):
            continue
        selected.append([fmt(point[unix_i]), point[value_i]])
    limit = clamp(limit, 1, MAX_POINTS)
    return {"points": selected[-limit:], "matching_points": len(selected), "truncated": len(selected) > limit}


def drop_trailing_placeholder(points: list[list], types: list[str], column: str) -> tuple[list[list], bool]:
    """Drop a final 0 following a non-zero value.

    Metric files can end on a zero row for a day that is not finished. A real drop
    to exactly zero between consecutive periods does not happen on a live chain.
    """
    if len(points) < 2:
        return points, False
    unix_i, value_i = types.index("unix"), types.index(column)
    ordered = sorted(points, key=lambda point: point[unix_i])
    if ordered[-1][value_i] == 0 and ordered[-2][value_i]:
        return ordered[:-1], True
    return points, False


class GtpData:
    def __init__(self, client: Optional[ApiClient] = None, live_client: Optional[ApiClient] = None):
        self.api = client or ApiClient()
        self.live = live_client or ApiClient(base_url=LIVE_BASE_URL, ttl=LIVE_CACHE_TTL_SECONDS)

    async def _master(self) -> dict:
        return await self.api.get("master.json")

    async def _chains(self) -> dict[str, dict]:
        chains = (await self._master())["chains"]
        return {key: chain for key, chain in chains.items()
                if key not in NON_CHAIN_KEYS and chain.get("deployment") == "PROD"}

    async def _chain(self, chain: str) -> tuple[str, dict]:
        key = str(chain).strip().lower()
        chains = await self._chains()
        if key not in chains:
            raise DataError(f"Unknown chain {chain!r}. Valid origin keys: {', '.join(sorted(chains))}")
        return key, chains[key]

    async def _projects(self) -> dict[str, dict]:
        data = await self.api.get("labels/projects.json")
        return {row["owner_project"]: row for row in rows(data["data"])}

    async def _project(self, owner_project: str) -> tuple[str, dict]:
        key = str(owner_project).strip().lower()
        projects = await self._projects()
        if key not in projects:
            raise DataError(f"Unknown app {owner_project!r}. Use search_apps to find its owner_project key.")
        return key, projects[key]

    async def list_chains(self) -> dict:
        chains = [{
            "origin_key": key,
            "name": chain.get("name"),
            "type": chain.get("technology") or chain.get("chain_type"),
            "stack": (chain.get("stack") or {}).get("label"),
            "da_layer": chain.get("da_layer"),
            "gas_token": chain.get("gas_token"),
            "launch_date": chain.get("launch_date"),
            "maturity": chain.get("maturity"),
            "l2beat_stage": (chain.get("l2beat_stage") or {}).get("stage"),
            "evm_chain_id": chain.get("evm_chain_id"),
            "metrics": chain.get("supported_metrics"),
        } for key, chain in (await self._chains()).items()]
        return {"chains": chains, "source": SOURCE}

    async def list_metrics(self) -> dict:
        master = await self._master()

        def describe(metrics: dict) -> list[dict]:
            described = []
            for key, metric in metrics.items():
                item = {"metric": key, "name": metric.get("name"), "units": list(metric.get("units", {}))}
                if metric.get("description"):
                    item["description"] = metric["description"]
                described.append(item)
            return described

        return {"chain_metrics": describe(master["metrics"]), "app_metrics": describe(master["app_metrics"]),
                "chain_granularities": list(GRANULARITIES), "app_periods": list(APP_PERIODS), "source": SOURCE}

    async def get_chain_metric(self, chain: str, metric: str, granularity: str = "daily", unit: str = "usd",
                               start_date: Optional[str] = None, end_date: Optional[str] = None,
                               limit: int = 90) -> dict:
        key, info = await self._chain(chain)
        choose(granularity, GRANULARITIES, "granularity")
        supported = info.get("supported_metrics") or []
        if metric not in supported:
            raise DataError(f"{key} has no metric {metric!r}. Available: {', '.join(supported)}")
        data = await self.api.get(f"metrics/chains/{key}/{metric}.json")
        details = data["details"]
        series = details["timeseries"].get(granularity)
        if not series:
            raise DataError(f"{metric} on {key} has no {granularity} series. Has: {', '.join(details['timeseries'])}")
        types = series["types"]
        column = value_column(types, unit)
        points, placeholder = drop_trailing_placeholder(series["data"], types, column)
        definition = (await self._master())["metrics"].get(metric, {})
        result = {
            "chain": key,
            "metric": metric,
            "metric_name": details.get("metric_name"),
            "granularity": granularity,
            "unit": column,
            "unit_suffix": (definition.get("units", {}).get(column) or {}).get("suffix"),
            **slice_series(points, types, column, start_date, end_date, limit, granularity == "hourly"),
        }
        summary = {period: single_row(values).get(column) for period, values in (details.get("summary") or {}).items()}
        result["recent_summary"] = {period: value for period, value in summary.items() if value is not None}
        result["recent_summary_aggregation"] = SUMMARY_AGGREGATION.get(definition.get("monthly_agg"))
        changes = (details.get("changes") or {}).get(granularity) or {}
        change_types = changes.get("types", [])
        if column in change_types:
            index = change_types.index(column)
            result["change_ratio"] = {period: values[index] for period, values in changes.items() if period != "types"}
        if placeholder:
            # The published 1d figures are computed from the same zero row.
            result["recent_summary"].pop("last_1d", None)
            result.pop("change_ratio", None)
            result["note"] = "The newest day has no final value yet and was left out."
        result.update(last_updated_utc=data.get("last_updated_utc"), source=SOURCE)
        return result

    async def compare_chains(self, metric: str, on_date: Optional[str] = None, limit: int = 30) -> dict:
        master = await self._master()
        choose(metric, list(master["metrics"]), "metric")
        metric_key = master["metrics"][metric]["metric_keys"][0]
        chains = await self._chains()
        fundamentals = await self.api.get("fundamentals.json")
        by_date: dict[str, dict[str, float]] = {}
        for row in fundamentals:
            if row["metric_key"] == metric_key and row["origin_key"] in chains and row["value"] is not None:
                by_date.setdefault(row["date"], {})[row["origin_key"]] = row["value"]
        if not by_date:
            raise DataError(f"No cross-chain data for {metric!r}")
        if on_date:
            parse_date(on_date, "date")
            if on_date not in by_date:
                raise DataError(f"No {metric} data for {on_date}. Available {min(by_date)} to {max(by_date)}.")
            chosen = on_date
        else:
            most_chains = max(len(values) for values in by_date.values())
            chosen = max(day for day, values in by_date.items() if len(values) >= MIN_DATE_COVERAGE * most_chains)
        ranked = sorted(by_date[chosen].items(), key=lambda item: item[1], reverse=True)
        return {
            "metric": metric,
            "metric_key": metric_key,
            "date": chosen,
            "chains_reporting": len(ranked),
            "ranking": [{"rank": i, "chain": chain, "value": value}
                        for i, (chain, value) in enumerate(ranked[:clamp(limit, 1, 50)], 1)],
            "source": SOURCE,
        }

    async def get_chain_overview(self, chain: str) -> dict:
        key, _ = await self._chain(chain)
        payload = await self.api.get(f"chains/{key}/overview.json")
        data = payload["data"]
        ranking = {metric: {k: v for k, v in entry.items() if k != "color_scale" and v is not None}
                   for metric, entry in (data.get("ranking") or {}).items()}
        kpis = {}
        for metric, card in (data.get("kpi_cards") or {}).items():
            kpis[metric] = {"current": single_row(card.get("current_values")),
                            "week_over_week_ratio": single_row(card.get("wow_change"))}
        events = sorted(data.get("events") or [], key=lambda e: e.get("date") or "", reverse=True)[:5]
        ecosystem = data.get("ecosystem") or {}
        return {
            "chain": key,
            "chain_name": data.get("chain_name"),
            "ranking": ranking,
            "kpis": kpis,
            "blockspace_by_category": rows(data.get("blockspace")),
            "active_apps": (ecosystem.get("active_apps") or {}).get("count"),
            "top_apps": rows(ecosystem.get("apps"))[:10],
            "recent_events": [{"date": e.get("date"), "title": e.get("title"), "type": e.get("type")} for e in events],
            "last_updated_utc": payload.get("last_updated_utc"),
            "source": SOURCE,
        }

    async def get_current_fees(self, chain: Optional[str] = None) -> dict:
        key = (await self._chain(chain))[0] if chain else None
        payload = await self.api.get("fees/table.json")
        chain_data = payload["chain_data"]
        if key and key not in chain_data:
            raise DataError(f"No fee data for {key}. Available: {', '.join(sorted(chain_data))}")

        def latest(metric: dict, column: str) -> Optional[float]:
            table = rows(metric)
            return max(table, key=lambda r: r["unix"]).get(column) if table else None

        result = []
        for origin, entry in chain_data.items():
            if key and origin != key:
                continue
            hourly = entry.get("hourly") or {}
            newest = rows(hourly.get("txcosts_median"))
            result.append({
                "chain": origin,
                "as_of": to_hour(max(r["unix"] for r in newest)) if newest else None,
                # value_usd is rounded to 4 decimals, so very cheap chains show 0.0 there.
                "median_tx_cost_usd": latest(hourly.get("txcosts_median"), "value_usd"),
                "median_tx_cost_eth": latest(hourly.get("txcosts_median"), "value_eth"),
                "avg_tx_cost_usd": latest(hourly.get("txcosts_avg"), "value_usd"),
                "swap_cost_usd": latest(hourly.get("txcosts_swap"), "value_usd"),
                "tps": latest(hourly.get("tps"), "value"),
                "throughput_mgas_per_second": latest(hourly.get("throughput"), "value"),
            })
        result.sort(key=lambda r: (r["median_tx_cost_eth"] is None, r["median_tx_cost_eth"] or 0))
        return {"chains": result, "source": SOURCE}

    async def get_live_chain_stats(self, chain: Optional[str] = None) -> dict:
        payload = await self.live.get("api/data")
        chains = payload["data"]
        now_ms = time.time() * 1000

        def stats(key: str, entry: dict) -> dict:
            age = (now_ms - entry["timestamp"]) / 1000 if entry.get("timestamp") else None
            return {
                "chain": key,
                "name": entry.get("display_name"),
                "tps": entry.get("tps"),
                "block_time_seconds": entry.get("block_time"),
                # 0 means the feed has no cost for this chain, not that transactions are free.
                **{field: entry.get(source) or None for field, source in LIVE_COST_FIELDS.items()},
                "age_seconds": round(age, 1) if age is not None else None,
                "stale": bool(age is None or age > LIVE_STALE_SECONDS),
            }

        if chain:
            key = str(chain).strip().lower()
            if key not in chains:
                raise DataError(f"No live data for {chain!r}. Available: {', '.join(sorted(chains))}")
            detail = await self.live.get(f"api/chain/{key}")
            records = detail.get("metrics") or {}
            return {**stats(key, detail["data"]),
                    "tps_ath": records.get("ath"), "tps_ath_time": records.get("ath_timestamp"),
                    "tps_24h_high": records.get("24h_high"), "tps_24h_high_time": records.get("24h_high_timestamp"),
                    "note": LIVE_NOTE, "source": SOURCE}
        overall = payload.get("global_metrics") or {}
        ordered = sorted((stats(key, entry) for key, entry in chains.items()), key=lambda s: s["tps"] or 0, reverse=True)
        return {
            "total_tps": overall.get("total_tps"),
            "total_tps_ath": overall.get("total_tps_ath"),
            "total_tps_24h_high": overall.get("total_tps_24h_high"),
            "active_chains": overall.get("active_chains"),
            "ethereum_tx_cost_usd": overall.get("ethereum_tx_cost_usd"),
            "layer2s_tx_cost_usd": overall.get("layer2s_tx_cost_usd"),
            "chains": ordered,
            "note": LIVE_NOTE,
            "source": SOURCE,
        }

    @staticmethod
    def _economics_row(chain: str, period: dict, unit: str) -> dict:
        index = 0 if unit == "usd" else 1
        costs = period.get("costs") or {}

        def figure(block: Optional[dict], field: str = "total") -> Optional[float]:
            values = (block or {}).get(field)
            return values[index] if values else None

        return {
            "chain": chain,
            "revenue": figure(period.get("revenue")),
            "costs": figure(costs),
            "costs_l1": figure(costs, "costs_l1"),
            "costs_blobs": figure(costs, "costs_blobs"),
            "profit": figure(period.get("profit")),
            "profit_margin": ((period.get("profit_margin") or {}).get("total") or [None])[0],
            "data_posted_bytes": ((period.get("size") or {}).get("total") or [None])[0],
        }

    async def get_l2_economics(self, chain: Optional[str] = None, period: str = "30d", unit: str = "usd") -> dict:
        choose(period, APP_PERIODS, "period")
        choose(unit, ("usd", "eth"), "unit")
        payload = await self.api.get("economics.json")
        breakdown = payload["data"]["chain_breakdown"]
        chains = [key for key in breakdown if key != "totals"]
        result: dict[str, Any] = {"period": period, "unit": unit, "note": ECONOMICS_NOTE}
        if chain:
            key = str(chain).strip().lower()
            choose(key, chains, "chain")
            result["economics"] = self._economics_row(key, breakdown[key][period], unit)
        else:
            rows_ = [self._economics_row(key, breakdown[key][period], unit) for key in chains if period in breakdown[key]]
            result["chains"] = sorted(rows_, key=lambda row: row["profit"] or 0, reverse=True)
            result["total"] = self._economics_row("all_l2s", breakdown["totals"][period], unit)
        result.update(last_updated_utc=payload.get("last_updated_utc"), source=SOURCE)
        return result

    async def get_l2_economics_timeseries(self, metric: str, chain: Optional[str] = None, granularity: str = "daily",
                                          unit: str = "usd", start_date: Optional[str] = None,
                                          end_date: Optional[str] = None, limit: int = 90) -> dict:
        choose(metric, ("revenue", "costs", "profit"), "metric")
        choose(granularity, ("daily", "monthly"), "granularity")
        choose(unit, ("usd", "eth"), "unit")
        payload = await self.api.get("economics.json")
        data = payload["data"]
        if chain:
            key = str(chain).strip().lower()
            choose(key, [name for name in data["chain_breakdown"] if name != "totals"], "chain")
            series = [data["chain_breakdown"][key][granularity][metric]]
        else:
            key = "all_l2s"
            metrics = data["all_l2s"]["metrics"]
            if metric == "costs":
                series = [metrics["costs"][part][granularity] for part in ("costs_l1", "costs_blobs")]
            else:
                series = [metrics["fees" if metric == "revenue" else "profit"][granularity]]
        types = series[0]["types"]
        unix_i, value_i = types.index("unix"), types.index(unit)
        merged: dict[float, float] = {}
        for item in series:
            for point in item["data"]:
                timestamp = normalize_unix(point[unix_i])
                if point[value_i] is not None:
                    merged[timestamp] = merged.get(timestamp, 0) + point[value_i]
        points, _ = drop_trailing_placeholder([[unix, value] for unix, value in merged.items()], ["unix", unit], unit)
        return {
            "metric": metric,
            "chain": key,
            "granularity": granularity,
            "unit": unit,
            **slice_series(points, ["unix", unit], unit, start_date, end_date, limit, False),
            "last_updated_utc": payload.get("last_updated_utc"),
            "source": SOURCE,
        }

    async def get_top_contracts(self, chain: str, sort_by: str = "txcount", limit: int = 25) -> dict:
        key, _ = await self._chain(chain)
        choose(sort_by, ("txcount", "gas_fees_usd"), "sort_by")
        contracts = await self.api.get(f"top_contracts/export_{key}.json")
        ordered = sorted(contracts, key=lambda item: item.get(f"{sort_by}_180d") or 0, reverse=True)
        limit = clamp(limit, 1, 100)
        return {
            "chain": key,
            "sort_by": f"{sort_by}_180d",
            "contracts": [{"rank": i, **{field: item.get(field) for field in TOP_CONTRACT_FIELDS}}
                          for i, item in enumerate(ordered[:limit], 1)],
            "note": "Top 100 contracts by the last 180 days. Names and project labels come from Open Labels Initiative attestations.",
            "source": SOURCE,
        }

    async def search_apps(self, query: str, limit: int = 10) -> dict:
        needle = str(query).strip().lower()
        if len(needle) < 2:
            raise DataError("query must be at least 2 characters")
        matches = []
        for key, project in (await self._projects()).items():
            name = (project.get("display_name") or "").lower()
            if needle in (key, name):
                score = 3
            elif key.startswith(needle) or name.startswith(needle):
                score = 2
            elif needle in key or needle in name:
                score = 1
            elif needle in (project.get("description") or "").lower():
                score = 0
            else:
                continue
            matches.append((-score, key, project))
        matches.sort(key=lambda match: match[:2])
        return {
            "matches": [{
                "owner_project": key,
                "display_name": project.get("display_name"),
                "category": project.get("main_category"),
                "sub_category": project.get("sub_category"),
                "token": project.get("token_symbol"),
                "website": project.get("website"),
                "description": (project.get("description") or "")[:200] or None,
            } for _, key, project in matches[:clamp(limit, 1, 25)]],
            "total_matches": len(matches),
            "source": SOURCE,
        }

    async def top_apps(self, metric: str = "txcount", timeframe: str = "7d", chain: Optional[str] = None,
                       limit: int = 20) -> dict:
        choose(metric, APP_RANKING_COLUMNS, "metric")
        choose(timeframe, APP_OVERVIEW_TIMEFRAMES, "timeframe")
        key = (await self._chain(chain))[0] if chain else None
        payload = await self.api.get(f"apps/app_overview_{timeframe}.json")
        # There is no prev_gas_fees_usd column, so that change is measured in ETH.
        change_column = "gas_fees_eth" if metric == "gas_fees_usd" else metric
        totals: dict[str, dict] = {}
        for row in rows(payload["data"]):
            if key and row["origin_key"] != key:
                continue
            app = totals.setdefault(row["owner_project"], {"value": 0.0, "current": 0.0, "previous": 0.0, "chains": set()})
            app["value"] += row.get(metric) or 0
            app["current"] += row.get(change_column) or 0
            app["previous"] += row.get(f"prev_{change_column}") or 0
            app["chains"].add(row["origin_key"])
        projects = await self._projects()
        ranked = sorted(totals.items(), key=lambda item: item[1]["value"], reverse=True)[:clamp(limit, 1, 50)]
        result = {
            "metric": metric,
            "timeframe": timeframe,
            "chain": key or "all",
            "apps": [{
                "rank": i,
                "owner_project": name,
                "display_name": (projects.get(name) or {}).get("display_name"),
                "value": app["value"],
                "change_ratio": (app["current"] - app["previous"]) / app["previous"] if app["previous"] else None,
                "chains": sorted(app["chains"]),
            } for i, (name, app) in enumerate(ranked, 1)],
            "last_updated_utc": payload.get("last_updated_utc"),
            "source": SOURCE,
        }
        notes = []
        if metric == "daa":
            notes.append("Unique addresses over the timeframe." + (
                "" if key else " Summed per chain, so users active on several chains are counted once per chain."))
        if metric == "gas_fees_usd":
            notes.append("change_ratio is measured in ETH.")
        if notes:
            result["note"] = " ".join(notes)
        return result

    async def _app_details(self, key: str) -> dict:
        return await self.api.get(f"apps/details/{key}.json")

    async def get_app(self, owner_project: str, period: str = "30d") -> dict:
        key, project = await self._project(owner_project)
        choose(period, APP_PERIODS, "period")
        details = await self._app_details(key)
        metrics = {}
        for metric, entry in (details.get("metrics") or {}).items():
            aggregated = entry.get("aggregated") or {}
            types = aggregated.get("types", [])
            if not types:
                continue
            column = value_column(types, "usd")
            index = types.index(column)
            by_chain = {chain: periods[period][index] for chain, periods in (aggregated.get("data") or {}).items()
                        if periods.get(period)}
            ordered = dict(sorted(by_chain.items(), key=lambda item: item[1] or 0, reverse=True)[:10])
            summary = {"name": entry.get("metric_name"), "unit": column, "by_chain": ordered}
            if metric in APP_ADDITIVE_METRICS and metric != "stables_balance":
                summary["total"] = sum(value or 0 for value in by_chain.values())
            metrics[metric] = summary
        contracts = rows((details.get("contracts_table") or {}).get(period))
        return {
            "owner_project": key,
            "display_name": project.get("display_name"),
            "description": (project.get("description") or "")[:400] or None,
            "category": project.get("main_category"),
            "sub_category": project.get("sub_category"),
            "token": project.get("token_symbol"),
            "website": project.get("website"),
            "period": period,
            "metrics": metrics,
            "top_contracts": contracts[:10],
            "first_seen": details.get("first_seen"),
            "last_updated_utc": details.get("last_updated_utc"),
            "source": SOURCE,
        }

    async def get_app_timeseries(self, owner_project: str, metric: str, chain: Optional[str] = None,
                                 granularity: str = "daily", unit: str = "usd", start_date: Optional[str] = None,
                                 end_date: Optional[str] = None, limit: int = 90) -> dict:
        key, _ = await self._project(owner_project)
        choose(granularity, ("daily", "hourly"), "granularity")
        details = await self._app_details(key)
        app_metrics = details.get("metrics") or {}
        entry = app_metrics.get(metric)
        if not entry:
            raise DataError(f"{key} has no metric {metric!r}. Available: {', '.join(app_metrics)}")
        over_time = entry.get("over_time") or {}
        if chain:
            selected = [chain.strip().lower()]
            if selected[0] not in over_time:
                raise DataError(f"{key} has no {metric} data on {chain!r}. Available: {', '.join(sorted(over_time))}")
        elif "all" in over_time:
            selected = ["all"]
        elif metric in APP_ADDITIVE_METRICS:
            selected = list(over_time)
        else:
            raise DataError(f"{metric} cannot be summed across chains, pass one of: {', '.join(sorted(over_time))}")
        series = [over_time[name][granularity] for name in selected if over_time[name].get(granularity)]
        if not series:
            raise DataError(f"{key} has no {granularity} {metric} series")
        types = series[0]["types"]
        column = value_column(types, unit)
        unix_i, value_i = types.index("unix"), types.index(column)
        merged: dict[float, float] = {}
        for item in series:
            for point in item["data"]:
                if point[value_i] is not None:
                    merged[point[unix_i]] = merged.get(point[unix_i], 0) + point[value_i]
        return {
            "owner_project": key,
            "metric": metric,
            "metric_name": entry.get("metric_name"),
            "chain": selected[0] if len(selected) == 1 else "all",
            "granularity": granularity,
            "unit": column,
            **slice_series([[unix, value] for unix, value in merged.items()], ["unix", column], column,
                           start_date, end_date, limit, granularity == "hourly"),
            "last_updated_utc": details.get("last_updated_utc"),
            "source": SOURCE,
        }
