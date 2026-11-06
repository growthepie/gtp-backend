"""ETH the asset: slices of eth_asset/live.json, eth_asset/daily.json and eim/*.json.

Field meanings are documented in docs/eth_asset_methodology.md. Several figures come
from third parties, so `sources` is kept in every answer as a list of names.
"""

from __future__ import annotations

from typing import Any, Optional

from gtp_data import (ApiClient, DataError, SOURCE, choose, clamp, drop_trailing_placeholder, slice_series,
                      value_column)

LIVE = "eth_asset/live.json"
DAILY = "eth_asset/daily.json"
SUPPLY = "eim/eth_supply.json"
EXPORTED = "eim/eth_exported.json"

# Flow KPIs report a total for the selected window in `current`; all others report the latest value.
FLOW_KPIS = {"eth_burned"}
HOURLY_GRANULARITIES = {"hourly", "3_hours"}
# series -> (chart key in eim/eth_supply.json, name, unit). eth_issuance_rate is derived from the day-over-day
# change in 7d average supply, so it is net of burn. It is not gross issuance, hence the name here.
SUPPLY_SERIES = {
    "supply_history": ("eth_supply", "ETH Supply", "eth"),
    "net_supply_growth": ("eth_issuance_rate", "Net ETH supply growth, annualized (7d average, after burn)", "fraction"),
}
DAILY_SERIES = ("gas_paid", "eth_btc_ratio", *SUPPLY_SERIES)
MARKET_CATEGORIES = ("crypto", "equity", "commodity")

TODAY_NOTE = ("Cards are AI-written around a figure computed from growthepie data, and topics are selected with a "
              "preference for positive ETH developments. Treat them as highlights, not a balanced summary.")


def compact(value: Any) -> Any:
    """Drop logos and reduce source objects to their names, at any depth."""
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if key == "logo":
                continue
            if key in ("sources", "eth_price_sources") and isinstance(item, list):
                result[key] = [source.get("name") for source in item if isinstance(source, dict)]
            else:
                result[key] = compact(item)
        return result
    if isinstance(value, list):
        return [compact(item) for item in value]
    return value


class EthData:
    def __init__(self, api: ApiClient):
        self.api = api

    async def _section(self, path: str, name: str) -> tuple[dict, Optional[str]]:
        bundle = await self.api.get(path)
        section = bundle["data"][name]
        return section, (bundle.get("sections_last_updated_utc") or {}).get(name)

    async def _kpis(self) -> tuple[dict, Optional[str]]:
        section, updated = await self._section(LIVE, "kpis")
        return section["kpis"], updated

    async def get_overview(self) -> dict:
        kpis, kpis_updated = await self._kpis()
        snapshot = {}
        for key, kpi in kpis.items():
            timeframes = kpi["timeframes"]
            entry = {"name": kpi.get("name"), "unit": kpi.get("unit"),
                     "change_ratio": {tf: data.get("change_pct") for tf, data in timeframes.items()}}
            if key in FLOW_KPIS:
                entry["window_totals"] = {tf: data.get("current") for tf, data in timeframes.items()}
            else:
                entry["current"] = next(iter(timeframes.values())).get("current")
            snapshot[key] = entry
        stats, _ = await self._section(LIVE, "stats")
        properties, _ = await self._section(LIVE, "properties")
        productive, _ = await self._section(LIVE, "productive_asset")
        market, market_updated = await self._section(DAILY, "market_caps")
        stake = productive["productive_uses"]["stake"]
        return {
            "kpis": snapshot,
            "total_wallets": stats["total_wallets"]["value"],
            "eth_per_person": stats["eth_per_person"]["value"],
            "market_cap_usd": market["eth"]["market_cap_usd"],
            "eth_btc_market_cap_ratio": market["eth_btc_ratio"]["current"],
            "annual_stock_change": {key: {"value": asset.get("value"), "as_of": asset.get("as_of")}
                                    for key, asset in properties["scarcity"]["assets"].items()},
            "staking": {key: stake.get(key) for key in ("total_staked_eth", "active_validators", "entry_queue",
                                                        "exit_queue", "apr_breakdown")},
            "collateral_total_eth": properties["collateral"]["total_eth"],
            "last_updated_utc": kpis_updated,
            "market_caps_updated_utc": market_updated,
            "source": SOURCE,
        }

    async def _daily_series(self, series: str) -> tuple[dict, Optional[str], list]:
        if series == "gas_paid":
            section, updated = await self._section(LIVE, "properties")
            return section["gas_paid"], updated, section["gas_paid"].get("sources", [])
        if series == "eth_btc_ratio":
            section, updated = await self._section(DAILY, "market_caps")
            return section["eth_btc_ratio"], updated, section["eth_btc_ratio"].get("sources", [])
        payload = await self.api.get(SUPPLY)
        key, name, unit = SUPPLY_SERIES[series]
        return ({**payload["data"]["chart"][key]["daily"], "name": name, "unit": unit},
                payload.get("last_updated_utc"), [{"name": "ultrasound.money"}])

    async def get_timeseries(self, series: str, timeframe: str = "60d", unit: str = "eth",
                             start_date: Optional[str] = None, end_date: Optional[str] = None,
                             limit: int = 90) -> dict:
        kpis, updated = await self._kpis()
        result: dict[str, Any] = {"series": series}
        if series in kpis:
            kpi = kpis[series]
            choose(timeframe, list(kpi["timeframes"]), "timeframe")
            data = kpi["timeframes"][timeframe]
            granularity = data.get("granularity")
            result.update(name=kpi.get("name"), unit=kpi.get("unit"), timeframe=timeframe, current=data.get("current"),
                          current_meaning="total over the timeframe" if series in FLOW_KPIS else "latest value",
                          change_ratio=data.get("change_pct"))
            sources = kpi.get("sources", [])
        elif series in DAILY_SERIES:
            data, updated, sources = await self._daily_series(series)
            granularity = data.get("granularity", "daily")
            result.update(name=data.get("name", series), unit=data.get("unit"))
        else:
            raise DataError(f"Unknown series {series!r}. Valid values: {', '.join([*kpis, *DAILY_SERIES])}")
        types = data["types"]
        column = value_column(types, unit)
        points, _ = drop_trailing_placeholder(data["data"], types, column)
        if column != "value":
            result["unit"] = column
        result.update(granularity=granularity,
                      **slice_series(points, types, column, start_date, end_date, limit,
                                     granularity in HOURLY_GRANULARITIES))
        result.update(sources=compact({"sources": sources})["sources"], last_updated_utc=updated, source=SOURCE)
        return result

    async def get_yields(self) -> dict:
        productive, updated = await self._section(LIVE, "productive_asset")
        properties, _ = await self._section(LIVE, "properties")
        return {
            "yield_comparison": compact(productive["yield_comparison"]["assets"]),
            "staking_yield_breakdown": compact(properties["yield_breakdown"]),
            "productive_uses": compact(productive["productive_uses"]),
            "last_updated_utc": updated,
            "source": SOURCE,
        }

    async def get_supply_flow(self) -> dict:
        section, updated = await self._section(DAILY, "supply_flow")
        properties, _ = await self._section(LIVE, "properties")
        return {
            "supply": section.get("supply"),
            "break_even_burn_per_week": section.get("break_even_burn_per_week"),
            "break_even_demand_level": section.get("break_even_demand_level"),
            "scenarios": [{key: value for key, value in scenario.items() if key != "daily"}
                          for scenario in section.get("scenarios", [])],
            "annual_stock_change": compact(properties["scarcity"]["assets"]),
            "sources": compact({"sources": section.get("sources", [])})["sources"],
            "last_updated_utc": updated,
            "source": SOURCE,
        }

    async def get_market_comparison(self, category: Optional[str] = None, limit: int = 20) -> dict:
        section, updated = await self._section(DAILY, "market_caps")
        categories = section["categories"]
        if category:
            choose(category, MARKET_CATEGORIES, "category")
        # The crypto list already contains ETH; it must count once whichever category is compared.
        selected = [item for key, items in categories.items() if not category or key == category for item in items
                    if item.get("key") != "eth"]
        ethereum = {"key": "eth", "name": "Ethereum", "category": "crypto",
                    "market_cap_usd": section["eth"]["market_cap_usd"], "sources": section["eth"].get("sources", [])}
        ranked = sorted([ethereum, *selected], key=lambda item: item.get("market_cap_usd") or 0, reverse=True)
        eth_rank = next(i for i, item in enumerate(ranked, 1) if item is ethereum)
        limit = clamp(limit, 1, 80)
        entries = [{"rank": i, **compact(item)} for i, item in enumerate(ranked, 1)
                   if i <= limit or item is ethereum]
        return {"category": category or "all", "eth_rank": eth_rank, "assets_compared": len(ranked), "ranking": entries,
                "last_updated_utc": updated, "source": SOURCE}

    async def get_purchasing_power(self, region: Optional[str] = None) -> dict:
        section, updated = await self._section(DAILY, "purchasing_power")
        regions = section["regions"]
        if region:
            choose(region, list(regions), "region")
            regions = {region: regions[region]}
        return {"year_ago_date": section.get("year_ago_date"), "regions": compact(regions),
                "last_updated_utc": updated, "source": SOURCE}

    async def get_today(self, limit: int = 7) -> dict:
        section, updated = await self._section(DAILY, "today")
        cards = [{key: compact(card.get(key)) for key in ("date", "data_date", "headline", "sentence", "metric",
                                                          "value", "unit", "value_formatted")}
                 | {"sources": compact({"sources": card.get("sources", [])})["sources"]}
                 for card in section.get("cards", [])[:clamp(limit, 1, 30)]]
        return {"cards": cards, "note": TODAY_NOTE, "last_updated_utc": updated, "source": SOURCE}

    async def get_exported(self, entity: Optional[str] = None, unit: str = "eth", start_date: Optional[str] = None,
                           end_date: Optional[str] = None, limit: int = 90) -> dict:
        payload = await self.api.get(EXPORTED)
        data = payload["data"]
        chart, entities = data["chart"], data["entities"]
        choose(unit, ("usd", "eth"), "unit")

        def changes(item: dict) -> dict:
            table = item.get("changes") or {}
            types = table.get("types", [])
            if unit not in types:
                return {}
            index = types.index(unit)
            return {period: values[index] for period, values in table.items() if period != "types"}

        meta = lambda key: {"name": entities.get(key, {}).get("name"), "type": entities.get(key, {}).get("type")}  # noqa: E731
        if entity is None:
            ranked = sorted(chart.items(), key=lambda item: item[1].get("latest_eth") or 0, reverse=True)
            return {
                "metric": data.get("metric_name"),
                "unit": unit,
                "entities": [{"entity": key, **meta(key), "latest_eth": item.get("latest_eth"),
                              "latest_usd": item.get("latest_usd"), "change_ratio": changes(item)}
                             for key, item in ranked[:clamp(limit, 1, 40)]],
                "last_updated_utc": payload.get("last_updated_utc"),
                "source": SOURCE,
            }
        choose(entity, list(chart), "entity")
        item = chart[entity]
        series = item["daily"]
        return {
            "entity": entity,
            **meta(entity),
            "unit": unit,
            **slice_series(series["data"], series["types"], unit, start_date, end_date, limit, False),
            "latest_eth": item.get("latest_eth"),
            "latest_usd": item.get("latest_usd"),
            "change_ratio": changes(item),
            "last_updated_utc": payload.get("last_updated_utc"),
            "source": SOURCE,
        }
