"""Quick bites: one reader over the small topic files under quick-bites/.

Only datasets in QUICK_BITES can be read, and a dataset's `key` is checked against
that topic's published values before it goes in a URL. Files share three shapes:
{types, values} tables, {columns, rows} tables, and flat KPI objects.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Optional

from gtp_data import ApiClient, DataError, SOURCE, choose, clamp, normalize_unix, to_date

# Chart styling and images, which only cost tokens.
NOISE_FIELD = re.compile(r"^(colors?|logos?|logo_path|image|icon_type)$")
# Agent metadata is free text anyone can write onchain: it can hold prompt injections and
# personal contact details, so only ids and counts are passed on.
EIP8004_UNSAFE_FIELDS = re.compile(r"^(name|description|service_\w+_endpoint)$")
MAX_RESPONSE_BYTES = 40_000
MAX_LISTED_KEYS = 150


@dataclass(frozen=True)
class Dataset:
    path: str
    description: str
    drop_fields: Optional[re.Pattern] = None
    key: Optional[str] = None  # name of the entry in KEY_SOURCES


# key name -> (dropdown path and field) or fixed values.
KEY_SOURCES: dict[str, tuple[str, str] | tuple[str, ...]] = {
    "stablecoin_chain": ("stablecoins/dropdown-chains", "origin_key"),
    "fiat": ("stablecoins/dropdown-fiat", "fiat"),
    "issuer": ("stablecoins/dropdown-projects", "owner_project"),
    "ticker": ("robinhood/dropdown", "ticker"),
    "sourcify_chain": ("sourcify/dropdown-chains", "origin_key"),
}
FUSAKA_ERAS = ("Fusaka-BPO2", "Fusaka-BPO1", "Fusaka", "Pectra", "Dencun")

QUICK_BITES: dict[str, dict[str, Dataset]] = {
    "stablecoins": {
        "chain_timeseries": Dataset("stablecoins/chains/top_{key}", "Top stablecoin supply on a chain over time", key="stablecoin_chain"),
        "chain_table": Dataset("stablecoins/chains/table_{key}", "Every stablecoin on a chain, latest", key="stablecoin_chain"),
        "fiat_overview": Dataset("stablecoins/fiat/timeseries", "Supply by fiat currency over time"),
        "fiat_count": Dataset("stablecoins/fiat/unique-fiat-count", "Number of fiat currencies with a stablecoin over time"),
        "fiat_timeseries": Dataset("stablecoins/fiat/{key}", "Supply of one fiat currency by token, in USD", key="fiat"),
        "fiat_native_timeseries": Dataset("stablecoins/fiat/{key}_native", "Supply of one fiat currency by token, in that currency", key="fiat"),
        "fiat_table": Dataset("stablecoins/fiat/table_{key}", "Tokens of one fiat currency, latest", key="fiat"),
        "issuer_timeseries": Dataset("stablecoins/projects/{key}", "One issuer's supply over time", key="issuer"),
        "issuer_table": Dataset("stablecoins/projects/table_{key}", "One issuer's tokens, latest", key="issuer"),
    },
    "robinhood": {
        "kpis": Dataset("robinhood/kpi", "Tokenized stock totals and changes"),
        "totals": Dataset("robinhood/totals", "Total tokenized value over time"),
        "stocks": Dataset("robinhood/stock_table", "All tokenized stocks, latest"),
        "stock": Dataset("robinhood/stocks/{key}", "One tokenized stock over time", key="ticker"),
    },
    "eip8004": {
        "kpis": Dataset("eip8004/kpis", "Agent registry totals"),
        "top_agents": Dataset("eip8004/top_agents", "Top agents by feedback, ids and counts only", drop_fields=EIP8004_UNSAFE_FIELDS),
        "chains": Dataset("eip8004/origin_breakdown", "Registrations and feedback by chain"),
        "events": Dataset("eip8004/events_cumulative", "Registration and feedback events over time"),
        "registered_by_chain": Dataset("eip8004/registered_cumulative", "Registered agents by chain over time"),
        "services": Dataset("eip8004/service_counts", "Agents by service type"),
        "uri_quality": Dataset("eip8004/uri_quality", "Share of valid agent URIs"),
        "uri_quality_daily": Dataset("eip8004/invalid_uri_daily", "Valid and invalid URIs over time"),
    },
    "linea": {
        "kpis": Dataset("linea/kpis", "Linea burn and profit totals"),
        "burn": Dataset("linea/burn", "LINEA and ETH burned over time"),
        "profit": Dataset("linea/profit_calculation", "Gas income, operating costs, and burn amount over time"),
    },
    "pol_burn": {"timeseries": Dataset("pol-burn/timeseries", "POL burned on Polygon over time")},
    "hyperliquid": {
        "kpis": Dataset("hyperliquid/kpis", "Hyperliquid USDC and Circle revenue estimates"),
        "usdc": Dataset("hyperliquid/usdc", "Hyperliquid USDC and Arbitrum stablecoins over time"),
    },
    "fusaka": {
        "totals": Dataset("fusaka/totals", "Fusaka block, blob, and fee totals"),
        "eip7918_kpis": Dataset("fusaka/eip7918_kpis", "Blob fees with and without EIP-7918"),
        "eip7918_timeseries": Dataset("fusaka/eip7918_timeseries", "Blob fees with and without EIP-7918 over time"),
        "blobs": Dataset("fusaka/timeseries_blobs/{key}", "Blob usage and fees in one fork era", key="fusaka_era"),
        "gas": Dataset("fusaka/timeseries_gas/{key}", "Gas used, limit, and burn in one fork era", key="fusaka_era"),
    },
    "eigenda_megaeth": {
        "eigenda_capacity": Dataset("eigenda-megaeth/eigenda_max_capacity", "EigenDA maximum capacity over time"),
        "blob_throughput": Dataset("eigenda-megaeth/eth_blob_throughput", "Ethereum blob throughput over time"),
        "megaeth_throughput": Dataset("eigenda-megaeth/megaeth_eigenda_throughput", "MegaETH data posted to EigenDA over time"),
    },
    "argot": {
        "contracts": Dataset("argot/table_latest", "Top contracts by value, with compiler and version"),
        "compiler_contracts": Dataset("argot/compiler_ct_timeseries", "Contract count by compiler over time"),
        "compiler_value": Dataset("argot/compiler_tvs_timeseries", "Value secured by compiler over time"),
        "solc_contracts": Dataset("argot/solc_ct_timeseries", "Contract count by Solidity version over time"),
        "solc_value": Dataset("argot/solc_tvs_timeseries", "Value secured by Solidity version over time"),
        "vyper_contracts": Dataset("argot/vyper_ct_timeseries", "Contract count by Vyper version over time"),
        "vyper_value": Dataset("argot/vyper_tvs_timeseries", "Value secured by Vyper version over time"),
    },
    "sourcify": {
        "compiler": Dataset("sourcify/{key}_compiler_total", "Gas and transactions by compiler on a chain", key="sourcify_chain"),
        "solidity": Dataset("sourcify/{key}_solidity_version_total", "Gas and transactions by Solidity version on a chain", key="sourcify_chain"),
        "vyper": Dataset("sourcify/{key}_vyper_version_total", "Gas and transactions by Vyper version on a chain", key="sourcify_chain"),
    },
    "pectra": {"summary": Dataset("pectra-fork", "Blob count and target, and type 4 transactions by chain")},
    "arbitrum_timeboost": {"summary": Dataset("arbitrum-timeboost", "Arbitrum base and priority fees over time")},
    "shopify_usdc": {"summary": Dataset("shopify-usdc", "Shopify USDC volume, merchants, and payers")},
    "ethereum_scaling": {"summary": Dataset("ethereum-scaling/data", "Historical and projected Ethereum and L2 TPS")},
}
TOPIC_NOTES = {"eip8004": "Agent names, descriptions, and endpoints are left out because they are unverified onchain text."}


def _table_of(node: dict) -> Optional[tuple[list, list]]:
    if isinstance(node.get("columns"), list) and isinstance(node.get("rows"), list):
        return node["columns"], node["rows"]
    if isinstance(node.get("types"), list) and isinstance(node.get("values"), list):
        return node["types"], node["values"]
    return None


def _align_columns(columns: list, table_rows: list) -> list:
    """Name every value in a row, so that a value can never land under the wrong column.

    Some files leave the leading timestamp out of `types`, which shifts every other column by one.
    """
    if not table_rows or all(len(row) == len(columns) for row in table_rows):
        return columns
    if all(len(row) == len(columns) + 1 and isinstance(row[0], (int, float)) and row[0] > 1e11 for row in table_rows):
        return ["unix", *columns]
    raise DataError("This dataset has a table whose columns do not match its rows, so it cannot be read reliably.")


def _shape_table(columns: list, table_rows: list, limit: int, drop: Optional[re.Pattern]) -> dict:
    columns = _align_columns(columns, table_rows)
    keep = [i for i, name in enumerate(columns)
            if not NOISE_FIELD.match(str(name)) and not (drop and drop.match(str(name)))]
    names = [columns[i] for i in keep]
    time_column = None
    if "unix" in columns:
        time_column = columns.index("unix")
    elif "date" in columns and len({row[columns.index("date")] for row in table_rows}) == len(table_rows):
        time_column = columns.index("date")
    if time_column is not None:
        ordered = sorted(table_rows, key=lambda row: row[time_column])
        selected = ordered[-limit:]
    else:
        selected = table_rows[:limit]
    is_unix = columns[time_column] == "unix" if time_column is not None else False

    def cell(row: list, i: int) -> Any:
        return to_date(normalize_unix(row[i])) if is_unix and i == time_column else row[i]

    shaped = {"columns": ["date" if name == "unix" else name for name in names],
              "rows": [[cell(row, i) for i in keep] for row in selected],
              "total_rows": len(table_rows)}
    if len(selected) < len(table_rows):
        shaped["truncated"] = "newest rows kept" if time_column is not None else "first rows kept"
    return shaped


def shape(node: Any, limit: int, drop: Optional[re.Pattern] = None) -> Any:
    """Reduce a quick-bite file to bounded tables, keeping its structure."""
    if isinstance(node, dict):
        table = _table_of(node)
        if table:
            return _shape_table(*table, limit, drop)
        return {key: shape(value, limit, drop) for key, value in node.items()
                if not NOISE_FIELD.match(str(key)) and not (drop and drop.match(str(key)))}
    if isinstance(node, list):
        if len(node) > limit:
            return {"items": node[:limit], "total_items": len(node), "truncated": "first items kept"}
        return [shape(item, limit, drop) for item in node]
    return node


class QuickBites:
    def __init__(self, api: ApiClient):
        self.api = api

    async def _keys(self, name: str) -> list[str]:
        if name == "fusaka_era":
            return list(FUSAKA_ERAS)
        path, field = KEY_SOURCES[name]
        payload = await self.api.get(f"quick-bites/{path}.json")
        return [str(item[field]) for item in payload["dropdown_values"]]

    async def list(self, topic: Optional[str] = None) -> dict:
        if topic is None:
            return {"topics": [{"topic": name, "datasets": list(datasets)} for name, datasets in QUICK_BITES.items()],
                    "source": SOURCE}
        choose(topic, list(QUICK_BITES), "topic")
        datasets = []
        for name, dataset in QUICK_BITES[topic].items():
            entry: dict[str, Any] = {"dataset": name, "description": dataset.description}
            if dataset.key:
                keys = await self._keys(dataset.key)
                entry["keys"] = keys[:MAX_LISTED_KEYS]
                if len(keys) > MAX_LISTED_KEYS:
                    entry["total_keys"] = len(keys)
            datasets.append(entry)
        result = {"topic": topic, "datasets": datasets, "source": SOURCE}
        if topic in TOPIC_NOTES:
            result["note"] = TOPIC_NOTES[topic]
        return result

    async def get(self, topic: str, dataset: str, key: Optional[str] = None, limit: int = 30) -> dict:
        choose(topic, list(QUICK_BITES), "topic")
        choose(dataset, list(QUICK_BITES[topic]), "dataset")
        entry = QUICK_BITES[topic][dataset]
        path = entry.path
        if entry.key:
            if not key:
                raise DataError(f"{topic}/{dataset} needs a key. Use list_quick_bites with topic={topic!r} to see them.")
            keys = await self._keys(entry.key)
            if key not in keys:
                raise DataError(f"Unknown key {key!r} for {topic}/{dataset}. Use list_quick_bites with topic={topic!r}.")
            path = path.format(key=key)
        payload = await self.api.get(f"quick-bites/{path}.json")
        limit = clamp(limit, 1, 365)
        # A file can be wide as well as long, so rows alone do not bound the answer.
        body = shape(payload, limit, entry.drop_fields)
        while limit > 1 and len(json.dumps(body, default=str)) > MAX_RESPONSE_BYTES:
            limit //= 2
            body = shape(payload, limit, entry.drop_fields)
        if len(json.dumps(body, default=str)) > MAX_RESPONSE_BYTES:
            raise DataError(f"{topic}/{dataset} is too large to return.")
        result = {"topic": topic, "dataset": dataset, "key": key, "description": entry.description,
                  "row_limit_applied": limit, "result": body, "source": SOURCE}
        if topic in TOPIC_NOTES:
            result["note"] = TOPIC_NOTES[topic]
        return result
