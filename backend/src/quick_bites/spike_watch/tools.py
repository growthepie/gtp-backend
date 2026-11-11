"""Read-only tools the investigation agent can call, plus a store for the chart data it wants published.

Every tool returns text for the model. Nothing here writes to the database, the chain or S3:
chart data stays in memory until publish.py uploads it after the run.
"""
import json
import re
from datetime import datetime, timezone
from typing import Any

import requests
from sqlalchemy import text

from claude_agent_sdk import create_sdk_mcp_server, tool

MAX_TOOL_CHARS = 60_000
MAX_SQL_ROWS = 500
MAX_LOG_BLOCK_RANGE = 2_000
MAX_CHART_ROWS = 2_000

READ_RPC_METHODS = {
    "eth_blockNumber", "eth_chainId", "eth_getBlockByNumber", "eth_getBlockByHash",
    "eth_getBlockReceipts", "eth_getTransactionByHash", "eth_getTransactionReceipt",
    "eth_call", "eth_getLogs", "eth_getBalance", "eth_getCode", "eth_feeHistory",
}
SQL_START = re.compile(r"^\s*(with|select)\b", re.IGNORECASE)
SQL_FORBIDDEN = re.compile(r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|vacuum|call|do)\b", re.IGNORECASE)
CHART_NAME = re.compile(r"^[a-z0-9_]{3,40}$")


def _text(payload: Any) -> dict:
    out = payload if isinstance(payload, str) else json.dumps(payload, default=str)
    if len(out) > MAX_TOOL_CHARS:
        out = out[:MAX_TOOL_CHARS] + f"\n...[truncated, {len(out)} chars total; ask for less]"
    return {"content": [{"type": "text", "text": out}]}


def _error(message: str) -> dict:
    return {"content": [{"type": "text", "text": message}], "is_error": True}


class InvestigationTools:
    def __init__(self, db_connector):
        self.db = db_connector
        self.charts: dict[str, dict] = {}
        self._rpc_urls: dict[str, list[str]] = {}

    # ---------- RPC ----------
    def _rpcs(self, origin_key: str) -> list[str]:
        if origin_key not in self._rpc_urls:
            urls = []
            special = self.db.get_special_use_rpc(origin_key)  # synced, usually archive
            if special:
                urls.append(special)
            urls += [u for u in self.db.get_all_rpcs_for_chain(origin_key) if u not in urls]
            self._rpc_urls[origin_key] = urls
        return self._rpc_urls[origin_key]

    def _rpc(self, origin_key: str, method: str, params: list) -> Any:
        last_error = None
        for url in self._rpcs(origin_key)[:3]:
            try:
                r = requests.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=45)
                body = r.json()
                if "error" in body:
                    last_error = body["error"]
                    continue
                return body["result"]
            except Exception as e:
                last_error = str(e)
        raise RuntimeError(f"All RPCs failed for {origin_key}: {last_error}")

    def rpc_call(self, origin_key: str, method: str, params: list) -> dict:
        if method not in READ_RPC_METHODS:
            return _error(f"Method {method} is not allowed. Allowed: {sorted(READ_RPC_METHODS)}")
        if method == "eth_getLogs" and params:
            f = params[0]
            try:
                span = int(f.get("toBlock", "0x0"), 16) - int(f.get("fromBlock", "0x0"), 16)
            except (ValueError, TypeError, AttributeError):
                return _error("eth_getLogs needs hex fromBlock and toBlock.")
            if span > MAX_LOG_BLOCK_RANGE:
                return _error(f"eth_getLogs range is {span} blocks; keep it under {MAX_LOG_BLOCK_RANGE}.")
        if not self._rpcs(origin_key):
            return _error(f"No RPC configured for {origin_key} in sys_rpc_config.")
        try:
            return _text(self._rpc(origin_key, method, params))
        except Exception as e:
            return _error(str(e))

    def block_at_time(self, origin_key: str, iso_time: str) -> dict:
        """Binary search for the last block at or before a UTC time."""
        try:
            target = int(datetime.fromisoformat(iso_time.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp())
            lo, hi = 0, int(self._rpc(origin_key, "eth_blockNumber", []), 16)
            ts = lambda n: int(self._rpc(origin_key, "eth_getBlockByNumber", [hex(n), False])["timestamp"], 16)
            if ts(hi) <= target:
                return _text({"block": hi, "timestamp": ts(hi)})
            while hi - lo > 1:
                mid = (lo + hi) // 2
                if ts(mid) <= target:
                    lo = mid
                else:
                    hi = mid
            return _text({"block": lo, "timestamp": ts(lo)})
        except Exception as e:
            return _error(str(e))

    # ---------- SQL ----------
    def sql_read(self, query: str) -> dict:
        if not SQL_START.match(query) or SQL_FORBIDDEN.search(query) or ";" in query.strip().rstrip(";"):
            return _error("Only a single SELECT or WITH ... SELECT statement is allowed.")
        try:
            with self.db.engine.connect() as conn:
                with conn.begin():
                    conn.execute(text("SET TRANSACTION READ ONLY"))
                    conn.execute(text("SET LOCAL statement_timeout = '60s'"))
                    result = conn.execute(text(query.strip().rstrip(";")))
                    cols = list(result.keys())
                    rows = result.fetchmany(MAX_SQL_ROWS + 1)
            data = [dict(zip(cols, r)) for r in rows[:MAX_SQL_ROWS]]
            note = f" (first {MAX_SQL_ROWS} rows only)" if len(rows) > MAX_SQL_ROWS else ""
            return _text({"columns": cols, "rows": data, "note": note})
        except Exception as e:
            return _error(f"Query failed: {e}")

    # ---------- chart data ----------
    def save_chart_data(self, name: str, description: str, columns: list[str], rows: list[list]) -> dict:
        if not CHART_NAME.match(name):
            return _error("name must be 3-40 chars of a-z, 0-9 and _.")
        if not columns or columns[0] != "unix":
            return _error('The first column must be "unix" (UTC timestamp in milliseconds).')
        if not rows or len(rows) > MAX_CHART_ROWS:
            return _error(f"Provide 1 to {MAX_CHART_ROWS} rows.")
        for row in rows:
            if len(row) != len(columns):
                return _error(f"Every row needs {len(columns)} values: {columns}.")
            if not isinstance(row[0], (int, float)) or row[0] < 1e12:
                return _error("unix values must be milliseconds since epoch, e.g. 1761768000000.")
            if any(v is not None and not isinstance(v, (int, float)) for v in row[1:]):
                return _error("Values after unix must be numbers or null.")
        self.charts[name] = {"description": description, "types": columns, "values": sorted(rows, key=lambda r: r[0])}
        return _text(f"Saved chart data '{name}' with {len(rows)} rows. Use y_index 1..{len(columns) - 1} for {columns[1:]}.")

    def server(self):
        @tool("rpc_call", "Read-only JSON-RPC call on a chain growthepie tracks. Methods: " + ", ".join(sorted(READ_RPC_METHODS)) +
              f". eth_getLogs ranges are limited to {MAX_LOG_BLOCK_RANGE} blocks. Prefer eth_getBlockReceipts over many single receipts.",
              {"type": "object", "properties": {
                  "origin_key": {"type": "string", "description": "growthepie chain key, e.g. ethereum, base, arbitrum"},
                  "method": {"type": "string"},
                  "params": {"type": "array", "items": {}}},
               "required": ["origin_key", "method", "params"]})
        async def rpc_call(args):
            return self.rpc_call(args["origin_key"], args["method"], args["params"])

        @tool("block_at_time", "Find the last block at or before a UTC time (ISO 8601) on a chain.",
              {"origin_key": str, "iso_time": str})
        async def block_at_time(args):
            return self.block_at_time(args["origin_key"], args["iso_time"])

        @tool("sql_read", "Run one read-only SELECT on the growthepie Postgres database (60 s timeout, "
              f"first {MAX_SQL_ROWS} rows). Always filter by origin_key and a narrow date range: tables like "
              "blockspace_fact_contract_level are very large.",
              {"query": str})
        async def sql_read(args):
            return self.sql_read(args["query"])

        @tool("save_chart_data", "Store a time series to publish with the quick bite. First column must be "
              "'unix' (ms); the other columns are numeric series. Reference it later by name in the chart blocks.",
              {"type": "object", "properties": {
                  "name": {"type": "string"},
                  "description": {"type": "string"},
                  "columns": {"type": "array", "items": {"type": "string"}},
                  "rows": {"type": "array", "items": {"type": "array", "items": {"type": ["number", "null"]}}}},
               "required": ["name", "description", "columns", "rows"]})
        async def save_chart_data(args):
            return self.save_chart_data(args["name"], args["description"], args["columns"], args["rows"])

        return create_sdk_mcp_server(name="gtp_backend", version="1.0.0",
                                     tools=[rpc_call, block_at_time, sql_read, save_chart_data])
