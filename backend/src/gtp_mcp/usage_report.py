"""Usage report from the metering database, and API key creation.

  python usage_report.py --days 30          # text report
  python usage_report.py --days 30 --json   # same data as JSON
  python usage_report.py --new-key alice    # needs MCP_KEY_PEPPER
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import statistics
import sys
from datetime import datetime, timedelta, timezone


def query(connection: sqlite3.Connection, sql: str, *args) -> list[dict]:
    connection.row_factory = sqlite3.Row
    return [dict(row) for row in connection.execute(sql, args)]


def build_report(db_path: str, days: int) -> dict:
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
        calls = "FROM usage_events WHERE ts_utc >= ? AND method = 'tools/call'"
        # Keyed callers are grouped by key, anonymous callers by IP hash.
        caller = "COALESCE(key_name, ip_hash)"
        latencies = sorted(row["duration_ms"] for row in query(
            connection, f"SELECT duration_ms {calls} AND status = 'ok' AND duration_ms IS NOT NULL", since))
        totals = query(connection, f"""
            SELECT COUNT(*) AS tool_calls, COUNT(DISTINCT {caller}) AS unique_callers,
                   COUNT(DISTINCT key_name) AS active_keys, SUM(status != 'ok') AS not_ok,
                   SUM(status = 'rate_limited') AS rate_limited, COALESCE(SUM(response_bytes), 0) AS response_bytes
            {calls}""", since)[0]
        totals["not_ok"] = totals["not_ok"] or 0
        totals["rate_limited"] = totals["rate_limited"] or 0
        totals["latency_p50_ms"] = statistics.median(latencies) if latencies else None
        totals["latency_p95_ms"] = latencies[int(len(latencies) * 0.95)] if latencies else None
        return {
            "since": since,
            "totals": totals,
            "by_day": query(connection, f"""
                SELECT substr(ts_utc, 1, 10) AS day, COUNT(*) AS calls, COUNT(DISTINCT {caller}) AS callers
                {calls} GROUP BY day ORDER BY day""", since),
            "by_tool": query(connection, f"""
                SELECT tool, COUNT(*) AS calls, SUM(status != 'ok') AS not_ok, ROUND(AVG(duration_ms)) AS avg_ms,
                       ROUND(AVG(response_bytes)) AS avg_bytes
                {calls} GROUP BY tool ORDER BY calls DESC""", since),
            "by_key": query(connection, f"""
                SELECT COALESCE(key_name, '(anonymous)') AS key_name, COUNT(*) AS calls,
                       COUNT(DISTINCT ip_hash) AS ips, MAX(ts_utc) AS last_seen
                {calls} GROUP BY key_name ORDER BY calls DESC""", since),
            "by_client": query(connection, """
                SELECT COALESCE(client_name, user_agent, '(unknown)') AS client, COUNT(*) AS requests
                FROM usage_events WHERE ts_utc >= ? GROUP BY client ORDER BY requests DESC LIMIT 20""", since),
        }
    finally:
        connection.close()


def print_report(report: dict) -> None:
    print(f"Usage since {report['since']}")
    for name, value in report["totals"].items():
        print(f"  {name}: {value}")
    for section in ("by_day", "by_tool", "by_key", "by_client"):
        print(f"\n{section}")
        for row in report[section]:
            print("  " + "  ".join(f"{key}={value}" for key, value in row.items()))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default=os.getenv("MCP_USAGE_DB", "/data/usage.sqlite"))
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--new-key", metavar="NAME")
    args = parser.parse_args()
    if args.new_key:
        from metering import new_key
        pepper = os.getenv("MCP_KEY_PEPPER")
        if not pepper:
            print("MCP_KEY_PEPPER must be set", file=sys.stderr)
            return 1
        key, stored = new_key(pepper)
        print(f"Give this key to {args.new_key} (shown once): {key}")
        print(f"Add to MCP_API_KEYS_JSON: {json.dumps({args.new_key: stored})}")
        return 0
    report = build_report(args.db, args.days)
    print(json.dumps(report, indent=2)) if args.json else print_report(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
