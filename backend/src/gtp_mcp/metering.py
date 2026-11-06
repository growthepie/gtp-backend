"""Usage metering, rate limiting and optional API keys for the MCP server.

Every MCP request is recorded by MeteringMiddleware. Writes go through a bounded
queue to a background thread, so a slow disk or a full queue can never delay or
fail a tool call. Usage lands in a local SQLite file on purpose: this service is
public-facing and should not hold Postgres credentials.

Keys are optional. Without one a caller is counted by a salted hash of their IP
and by client name; raw IPs and raw keys are never stored. With one, usage is
attributed to the key's name and a higher rate limit applies.

Key format and hashing match builder_api (sha256 of "prefix.secret" + pepper). That
code cannot be imported here: it raises at import without BUILDER_* env vars, and
each service has its own Docker build context.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import queue
import secrets
import sqlite3
import threading
import time
from typing import Any, Optional

from mcp.shared.exceptions import MCPError

logger = logging.getLogger("gtp_mcp.metering")

KEY_NAMESPACE = "gtpmcp_"
CLIENT_INFO_META_KEY = "io.modelcontextprotocol/clientInfo"
# JSON-RPC server-defined error range.
ERROR_INVALID_KEY = -32001
ERROR_RATE_LIMITED = -32029
MAX_ARGS_CHARS = 500
MAX_TEXT_CHARS = 200

SCHEMA = """
CREATE TABLE IF NOT EXISTS usage_events (
    id INTEGER PRIMARY KEY,
    ts_utc TEXT NOT NULL,
    method TEXT NOT NULL,
    tool TEXT,
    arguments TEXT,
    key_name TEXT,
    ip_hash TEXT,
    client_name TEXT,
    client_version TEXT,
    user_agent TEXT,
    status TEXT NOT NULL,
    duration_ms REAL,
    response_bytes INTEGER
);
CREATE INDEX IF NOT EXISTS idx_usage_ts ON usage_events (ts_utc);
CREATE INDEX IF NOT EXISTS idx_usage_key ON usage_events (key_name, ts_utc);
"""
COLUMNS = ("ts_utc", "method", "tool", "arguments", "key_name", "ip_hash", "client_name", "client_version",
           "user_agent", "status", "duration_ms", "response_bytes")


def hash_key(prefix: str, secret: str, pepper: str) -> str:
    return hashlib.sha256(f"{prefix}.{secret}{pepper}".encode()).hexdigest()


def split_key(presented: str) -> Optional[tuple[str, str]]:
    if not presented.startswith(KEY_NAMESPACE):
        return None
    prefix, separator, secret = presented[len(KEY_NAMESPACE):].partition(".")
    return (prefix, secret) if prefix and separator and secret else None


def new_key(pepper: str) -> tuple[str, str]:
    """Return (key to hand to the user, 'prefix:hash' to store in MCP_API_KEYS_JSON)."""
    prefix, secret = secrets.token_hex(4), secrets.token_urlsafe(32)
    return f"{KEY_NAMESPACE}{prefix}.{secret}", f"{prefix}:{hash_key(prefix, secret, pepper)}"


class ApiKeys:
    """Key store built from MCP_API_KEYS_JSON, a {"name": "prefix:hash"} object."""

    def __init__(self, entries: dict[str, str], pepper: str):
        self.pepper = pepper
        self.by_prefix: dict[str, tuple[str, str]] = {}
        for name, value in entries.items():
            prefix, separator, key_hash = value.partition(":")
            if not separator or not prefix or not key_hash:
                raise RuntimeError(f"MCP_API_KEYS_JSON value for {name!r} must be prefix:hash")
            self.by_prefix[prefix] = (name, key_hash)
        if self.by_prefix and not pepper:
            raise RuntimeError("MCP_KEY_PEPPER must be set when MCP_API_KEYS_JSON has keys")

    @classmethod
    def from_env(cls) -> "ApiKeys":
        return cls(json.loads(os.getenv("MCP_API_KEYS_JSON") or "{}"), os.getenv("MCP_KEY_PEPPER", ""))

    def resolve(self, presented: str) -> Optional[str]:
        """Return the key's name, or None if the key is not valid."""
        parts = split_key(presented)
        if not parts or parts[0] not in self.by_prefix:
            return None
        name, expected = self.by_prefix[parts[0]]
        return name if hmac.compare_digest(hash_key(parts[0], parts[1], self.pepper), expected) else None


class RateLimiter:
    """Fixed one-minute window per caller. In memory, which is fine for one process."""

    def __init__(self, clock=time.time):
        self.clock = clock
        self.window = 0
        self.counts: dict[str, int] = {}

    def allow(self, identity: str, per_minute: int) -> bool:
        window = int(self.clock() // 60)
        if window != self.window:
            self.window, self.counts = window, {}
        count = self.counts.get(identity, 0)
        if count >= per_minute:
            return False
        self.counts[identity] = count + 1
        return True


class UsageMeter:
    """Append-only usage log. record() never blocks and never raises."""

    def __init__(self, path: str, max_queue: int = 10000):
        self.path = path
        self.dropped = 0
        self.events: queue.Queue = queue.Queue(maxsize=max_queue)
        self._closed = threading.Event()
        self._thread = threading.Thread(target=self._run, name="usage-meter", daemon=True)
        self._ready = threading.Event()
        self._thread.start()
        self._ready.wait(timeout=5)

    def record(self, **event: Any) -> None:
        try:
            self.events.put_nowait(tuple(event.get(column) for column in COLUMNS))
        except queue.Full:
            self.dropped += 1

    def _run(self) -> None:
        connection = None
        try:
            directory = os.path.dirname(self.path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            connection = sqlite3.connect(self.path)
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(SCHEMA)
        except (OSError, sqlite3.Error):
            logger.exception("usage metering disabled: cannot open %s", self.path)
            connection = None
        self._ready.set()
        insert = f"INSERT INTO usage_events ({', '.join(COLUMNS)}) VALUES ({', '.join('?' * len(COLUMNS))})"
        while not (self._closed.is_set() and self.events.empty()):
            batch = []
            try:
                batch.append(self.events.get(timeout=0.5))
                while len(batch) < 500:
                    batch.append(self.events.get_nowait())
            except queue.Empty:
                pass
            if not batch or connection is None:
                continue
            try:
                with connection:
                    connection.executemany(insert, batch)
            except sqlite3.Error:
                self.dropped += len(batch)
                logger.exception("failed to write %s usage events", len(batch))
        if connection is not None:
            connection.close()

    def close(self) -> None:
        self._closed.set()
        self._thread.join(timeout=5)


def _truncate(value: Any, limit: int = MAX_TEXT_CHARS) -> Optional[str]:
    return str(value)[:limit] if value else None


def _result_size_and_error(result: Any) -> tuple[Optional[int], bool]:
    if hasattr(result, "model_dump_json"):
        return len(result.model_dump_json(by_alias=True)), bool(getattr(result, "is_error", False))
    if isinstance(result, dict):
        return len(json.dumps(result, default=str)), bool(result.get("isError"))
    return None, False


class MeteringMiddleware:
    """MCP server middleware: resolves the caller, rate limits tool calls, records usage."""

    def __init__(self, meter: UsageMeter, keys: ApiKeys, limiter: RateLimiter, ip_salt: str,
                 anonymous_per_minute: int = 60, keyed_per_minute: int = 600, trust_proxy: bool = True):
        self.meter = meter
        self.keys = keys
        self.limiter = limiter
        self.ip_salt = ip_salt
        self.anonymous_per_minute = anonymous_per_minute
        self.keyed_per_minute = keyed_per_minute
        self.trust_proxy = trust_proxy

    def _ip_hash(self, request: Any) -> Optional[str]:
        address = request.client.host if request.client else None
        if self.trust_proxy:
            # Caddy replaces X-Forwarded-For with the real peer unless that peer is a trusted proxy.
            forwarded = request.headers.get("x-forwarded-for")
            if forwarded:
                address = forwarded.split(",")[-1].strip()
        if not address:
            return None
        return hmac.new(self.ip_salt.encode(), address.encode(), hashlib.sha256).hexdigest()[:16]

    @staticmethod
    def _presented_key(request: Any) -> Optional[str]:
        authorization = request.headers.get("authorization", "")
        if authorization.lower().startswith("bearer "):
            return authorization[7:].strip()
        return request.headers.get("x-api-key")

    @staticmethod
    def _client_info(ctx: Any, params: dict) -> dict:
        info = params.get("clientInfo") or (params.get("_meta") or {}).get(CLIENT_INFO_META_KEY)
        if isinstance(info, dict):
            return info
        client_params = getattr(ctx.session, "client_params", None)
        if client_params is not None and client_params.client_info is not None:
            return {"name": client_params.client_info.name, "version": client_params.client_info.version}
        return {}

    async def __call__(self, ctx: Any, call_next: Any) -> Any:
        if ctx.request_id is None:  # notifications are protocol noise, not usage
            return await call_next(ctx)
        started = time.perf_counter()
        params = dict(ctx.params or {})
        request = ctx.request
        presented = self._presented_key(request) if request is not None else None
        key_name = self.keys.resolve(presented) if presented else None
        ip_hash = self._ip_hash(request) if request is not None else None
        client = self._client_info(ctx, params)
        is_tool_call = ctx.method == "tools/call"

        def record(status: str, response_bytes: Optional[int] = None) -> None:
            arguments = params.get("arguments") if is_tool_call else None
            self.meter.record(
                ts_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                method=ctx.method,
                tool=_truncate(params.get("name")) if is_tool_call else None,
                arguments=json.dumps(arguments, default=str)[:MAX_ARGS_CHARS] if arguments else None,
                key_name=key_name,
                ip_hash=ip_hash,
                client_name=_truncate(client.get("name")),
                client_version=_truncate(client.get("version")),
                user_agent=_truncate(request.headers.get("user-agent")) if request is not None else None,
                status=status,
                duration_ms=round((time.perf_counter() - started) * 1000, 1),
                response_bytes=response_bytes,
            )

        if presented and key_name is None:
            # Fail loudly: silently falling back to anonymous would lose the attribution they asked for.
            record("invalid_key")
            raise MCPError(ERROR_INVALID_KEY, "Invalid API key")
        if is_tool_call:
            identity = f"key:{key_name}" if key_name else f"ip:{ip_hash}"
            per_minute = self.keyed_per_minute if key_name else self.anonymous_per_minute
            if not self.limiter.allow(identity, per_minute):
                record("rate_limited")
                raise MCPError(ERROR_RATE_LIMITED, f"Rate limit of {per_minute} tool calls per minute exceeded")
        try:
            result = await call_next(ctx)
        except Exception:
            record("error")
            raise
        size, is_error = _result_size_and_error(result)
        record("tool_error" if is_error else "ok", size)
        return result
