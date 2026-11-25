"""
Weekly public-API usage report built from CloudFront standard access logs.

Reads the log files of the API distribution (CF_DISTRIBUTION_ID) straight from S3
(bucket + prefix come from the distribution's own logging config), classifies every
request as our own traffic, bots or third parties, and builds a Discord summary of
who uses the API and which data they use.

Memory: each log file is folded into small counters as it is downloaded, so the
full set of raw rows is never held at once.
"""
import gzip
import io
import ipaddress
import os
import re
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from urllib.parse import unquote

import boto3
import pandas as pd

# Our own Airflow host (requests main_conf.pkl etc.). Override/extend with env GTP_OWN_IPS="ip1,ip2".
OWN_IPS = {"2a01:4f8:1c19:9a07::1", "46.224.123.169"}
# Exact user agents / prefixes that are our own services.
OWN_UA_EXACT = {"vercel-image-optimization/1.0"}
OWN_UA_PREFIX = ("growthepie/",)
# `node` with no referrer = Next.js server-side rendering of growthepie.com.
SSR_UA = "node"
OWN_REFERRER_RE = re.compile(r"growthepie|vercel\.app$|^localhost$|cloudfront\.net$")
BOT_RE = re.compile(r"bot|crawl|spider|slurp|facebookexternalhit|headless|lighthouse|preview|scanner", re.I)
GENERIC_UA_RE = re.compile(
    r"^(python|curl|go-http|axios|node-fetch|undici|okhttp|wget|aiohttp|java|dart|mozilla/5\.0$|-$|postman|deno|bun|got|ruby|php|libwww|apache-http|reqwest|rust|httr|guzzle)",
    re.I,
)
BROWSER_RE = re.compile(r"AppleWebKit|Gecko|Firefox")
COMPATIBLE_RE = re.compile(r"compatible; ([^;)]+)")
# Clients that hide behind a browser UA but were identified manually: (exact UA, IP prefix, name).
KNOWN_CLIENT_ALIASES = [
    ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Safari/537.36", "152.55.", "divisions.one"),
]
# WAF rule ms_RateLimiting blocks an IP above this many requests per 5-minute window.
WAF_RATE_LIMIT_PER_5MIN = 250

WANTED_FIELDS = ["date", "time", "sc-bytes", "c-ip", "cs-uri-stem", "sc-status", "cs(Referer)", "cs(User-Agent)"]


@lru_cache(maxsize=200_000)
def _net(ip: str) -> str:
    try:
        a = ipaddress.ip_address(ip)
        return str(ipaddress.ip_network(f"{ip}/{48 if a.version == 6 else 24}", strict=False))
    except ValueError:
        return ip


@lru_cache(maxsize=50_000)
def data_family(uri: str) -> str:
    """Group an endpoint into a data type, e.g. /v1/metrics/chains/base/daa.json -> 'chain metric: daa'."""
    u = re.sub(r"^/v1/", "", uri)
    patterns = [
        (r"metrics/chains/[^/]+/([^/.]+)", "chain metric: {}"),
        (r"metrics/([^/.]+)", "metric (all chains): {}"),
        (r"quick-bites/([^/]+)", "quick bite: {}"),
        (r"export/([^/.]+)", "export: {}"),
        (r"chains/[^/]+/([^/.]+)", "chain page: {}"),
    ]
    for pat, label in patterns:
        m = re.match(pat, u)
        if m:
            return label.format(m.group(1))
    if u.startswith("apps/details/"):
        return "app details"
    if u.startswith("apps/"):
        return "apps overview"
    if u.startswith(("labels/", "oli/")):
        return "labels / OLI: " + u.split("/")[-1]
    if u.startswith("fundamentals"):
        return u.split(".")[0]
    if "/" not in u and "." in u:
        return u.rsplit(".", 1)[0]
    return u.split("/")[0] or "/"


def classify(ip: str, ua: str, referrer: str, uri: str, own_ips: set):
    """Return (segment, client, kind). client/kind only set for third parties."""
    ref_dom = ""
    if referrer != "-":
        m = re.match(r"https?://([^/:]+)", referrer.lower())
        ref_dom = m.group(1) if m else referrer.lower()
    if ip in own_ips or ua in OWN_UA_EXACT or ua.startswith(OWN_UA_PREFIX):
        return "growthepie internal services", None, None
    if ref_dom and OWN_REFERRER_RE.search(ref_dom):
        return "growthepie frontend (browsers)", None, None
    if ua == SSR_UA and referrer == "-":
        return "growthepie frontend (server-side)", None, None
    if BOT_RE.search(ua):
        return "bots & crawlers", None, None
    if not uri.startswith("/v1/"):
        return "non-API paths (scanners etc.)", None, None
    for alias_ua, prefix, name in KNOWN_CLIENT_ALIASES:
        if ua == alias_ua and ip.startswith(prefix):
            return "third parties", name, "Named app"
    if ref_dom:
        return "third parties", ref_dom, "Website"
    if GENERIC_UA_RE.match(ua):
        tool = re.split(r"[/ ]", ua, maxsplit=1)[0] or "unknown"
        return "third parties", f"{tool} @ {_net(ip)}", "Anonymous script"
    if ua.startswith("Mozilla/5.0 (") and BROWSER_RE.search(ua) and "compatible;" not in ua:
        return "third parties", f"Browser UA @ {_net(ip)}", "Anonymous (browser UA)"
    m = COMPATIBLE_RE.search(ua)
    if m:
        return "third parties", m.group(1).strip(), "Named app"
    return "third parties", ua[:90], "Named app"


class _Agg:
    """Counters for one analysis window, filled file by file."""

    def __init__(self):
        self.segments = defaultdict(lambda: [0, 0])  # seg -> [requests, bytes]
        self.tp = defaultdict(lambda: [0, 0, 0])  # (client, kind, uri, date) -> [requests, ok, bytes]
        self.ssr_daily = defaultdict(lambda: [0, 0])  # date -> [requests, 403s]
        self.ssr_ip_5min = Counter()  # (ip, date, window) -> requests
        self.own_conf_pulls = 0


def _parse_file(text: str, agg_by_week: dict, own_ips: set):
    idx = None
    for line in text.splitlines():
        if line.startswith("#"):
            if line.startswith("#Fields:"):
                hdr = line.split()[1:]
                idx = [hdr.index(f) for f in WANTED_FIELDS]
            continue
        if idx is None:
            continue
        r = line.split("\t")
        d, t, b, ip, uri, status, ref, ua = (r[i] for i in idx)
        agg = agg_by_week.get(d)
        if agg is None:
            continue
        ua = unquote(unquote(ua))
        b = int(b) if b.isdigit() else 0
        seg, client, kind = classify(ip, ua, ref, uri, own_ips)
        s = agg.segments[seg]
        s[0] += 1
        s[1] += b
        if seg == "third parties":
            v = agg.tp[(client, kind, uri, d)]
            v[0] += 1
            v[1] += status in ("200", "304")
            v[2] += b
        elif seg == "growthepie frontend (server-side)":
            v = agg.ssr_daily[d]
            v[0] += 1
            v[1] += status == "403"
            agg.ssr_ip_5min[(ip, d, t[:3] + str(int(t[3:5]) // 5))] += 1
        elif seg == "growthepie internal services" and uri.endswith(".pkl"):
            agg.own_conf_pulls += 1


def collect(end_day: date, days: int = 7, workers: int = 16):
    """Aggregate the current window (end_day-days+1 .. end_day) and the window before it."""
    creds = dict(aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))
    dist_id = os.getenv("CF_DISTRIBUTION_ID")
    logging_cfg = boto3.client("cloudfront", **creds).get_distribution_config(Id=dist_id)["DistributionConfig"]["Logging"]
    bucket = logging_cfg["Bucket"].split(".s3")[0]
    prefix = logging_cfg["Prefix"].rstrip("/")
    prefix = f"{prefix}/" if prefix else ""
    own_ips = OWN_IPS | {x.strip() for x in os.getenv("GTP_OWN_IPS", "").split(",") if x.strip()}

    cur, prev = _Agg(), _Agg()
    agg_by_day = {}
    for i in range(days):
        agg_by_day[(end_day - timedelta(days=i)).isoformat()] = cur
        agg_by_day[(end_day - timedelta(days=days + i)).isoformat()] = prev

    s3 = boto3.client("s3", **creds)
    keys = []
    # Log files are named <dist>.<YYYY-MM-DD-HH>.<id>.gz in UTC; include one extra day on each
    # side because a file can hold a few lines from the neighbouring hour/day.
    first = end_day - timedelta(days=2 * days)
    for i in range(2 * days + 2):
        day = (first + timedelta(days=i)).isoformat()
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}{dist_id}.{day}"):
            keys += [o["Key"] for o in page.get("Contents", [])]
    print(f"Reading {len(keys)} log files from s3://{bucket}/{prefix}")

    def fetch(k):
        return gzip.decompress(s3.get_object(Bucket=bucket, Key=k)["Body"].read()).decode("utf-8", "replace")

    with ThreadPoolExecutor(workers) as ex:
        for text in ex.map(fetch, keys):
            _parse_file(text, agg_by_day, own_ips)
    return cur, prev


def _tp_frame(agg: _Agg) -> pd.DataFrame:
    rows = [(c, k, u, d, r, ok, b) for (c, k, u, d), (r, ok, b) in agg.tp.items()]
    df = pd.DataFrame(rows, columns=["client", "kind", "uri", "date", "requests", "ok", "bytes"])
    df["family"] = df.uri.map(data_family)
    return df


def _pct(new, old):
    if not old:
        return ""
    ch = 100 * (new - old) / old
    return f" ({ch:+.0f}% WoW)"


def _fmt(n):
    n = float(n)
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "k")):
        if abs(n) >= div:
            return f"{n / div:.1f}{suf}"
    return f"{n:.0f}"


def build_report(cur: _Agg, prev: _Agg, end_day: date, days: int = 7):
    """Return (list of Discord message chunks, client DataFrame for the CSV attachment)."""
    t, p = _tp_frame(cur), _tp_frame(prev)
    start_day = end_day - timedelta(days=days - 1)

    def client_table(df):
        if df.empty:
            return pd.DataFrame(columns=["client", "kind", "requests", "ok", "bytes", "days_active", "data_used"])
        g = df.groupby(["client", "kind"])
        out = g.agg(requests=("requests", "sum"), ok=("ok", "sum"), bytes=("bytes", "sum"), days_active=("date", "nunique")).reset_index()
        fam = df.groupby(["client", "family"]).requests.sum().reset_index().sort_values("requests", ascending=False)
        top_fam = fam.groupby("client").apply(
            lambda x: ", ".join(f"{f} {100 * r / x.requests.sum():.0f}%" for f, r in list(zip(x.family, x.requests))[:3] if r / x.requests.sum() >= 0.10)
        )
        out["data_used"] = out.client.map(top_fam)
        return out.sort_values("requests", ascending=False)

    c, cp = client_table(t), client_table(p)
    req, req_p = int(t.requests.sum()), int(p.requests.sum())
    ok_pct = 100 * t.ok.sum() / max(req, 1)
    recurring = int((c.days_active >= 3).sum())
    recurring_p = int((cp.days_active >= 3).sum())

    lines = [f"**📊 Public API usage — {start_day:%b %-d} to {end_day:%b %-d}**"]
    lines.append(
        f"Third parties: **{_fmt(req)}** requests{_pct(req, req_p)} · **{recurring}** recurring clients (3+ days){_pct(recurring, recurring_p)} · "
        f"{len(c):,} clients total · {ok_pct:.1f}% successful · {t.bytes.sum() / 1e9:.1f} GB"
    )

    # Own traffic health
    ssr_req = sum(v[0] for v in cur.ssr_daily.values())
    ssr_403 = sum(v[1] for v in cur.ssr_daily.values())
    peak_day, peak = max(cur.ssr_daily.items(), key=lambda kv: kv[1][0], default=(None, [0, 0]))
    over_limit = sum(1 for v in cur.ssr_ip_5min.values() if v > WAF_RATE_LIMIT_PER_5MIN)
    warn = ssr_req and (ssr_403 / ssr_req > 0.05 or over_limit > 0)
    lines.append("")
    lines.append(
        f"{'⚠️' if warn else '✅'} Our frontend server (node): {_fmt(ssr_req)} requests{_pct(ssr_req, sum(v[0] for v in prev.ssr_daily.values()))}, "
        f"{100 * ssr_403 / max(ssr_req, 1):.1f}% blocked (403) · peak day {peak_day} with {_fmt(peak[0])} · "
        f"{over_limit} IP×5min windows over the WAF limit of {WAF_RATE_LIMIT_PER_5MIN}"
    )
    lines.append(f"Airflow host .pkl pulls: {_fmt(cur.own_conf_pulls)}")

    # Data used
    fam = t.groupby("family").agg(clients=("client", "nunique"), requests=("requests", "sum")).sort_values("clients", ascending=False)
    fam_p = p.groupby("family").client.nunique() if not p.empty else pd.Series(dtype=int)
    lines.append("")
    lines.append("**Most used data (by clients)**")
    for i, (f, r) in enumerate(fam.head(8).iterrows(), 1):
        lines.append(f"{i}. `{f}` — {r.clients:,} clients, {_fmt(r.requests)} req{_pct(r.clients, fam_p.get(f, 0))}")

    # Top clients
    lines.append("")
    lines.append("**Top clients**")
    for i, r in enumerate(c.head(10).itertuples(), 1):
        lines.append(f"{i}. **{r.client[:60]}** ({r.kind}) — {_fmt(r.requests)} req, {r.days_active}d — {r.data_used or ''}")

    # New and gone heavy clients
    prev_clients = set(cp.client)
    new = c[(~c.client.isin(prev_clients)) & (c.requests >= 300)].head(8)
    if len(new):
        lines.append("")
        lines.append("**🆕 New heavy clients (300+ req, not seen last week)**")
        for r in new.itertuples():
            lines.append(f"• **{r.client[:60]}** ({r.kind}) — {_fmt(r.requests)} req — {r.data_used or ''}")
    gone = cp[(~cp.client.isin(set(c.client))) & (cp.requests >= 300)].head(5)
    if len(gone):
        lines.append("")
        lines.append("**👋 Stopped calling (300+ req last week, none this week)**")
        for r in gone.itertuples():
            lines.append(f"• {r.client[:60]} — {_fmt(r.requests)} req last week")

    # Endpoints that always fail but many clients want
    ep = t.groupby("uri").agg(clients=("client", "nunique"), requests=("requests", "sum"), ok=("ok", "sum"))
    failing = ep[(ep.ok == 0) & (ep.clients >= 10)].sort_values("clients", ascending=False).head(5)
    if len(failing):
        lines.append("")
        lines.append("**❌ Requested but always failing (missing file or blocked)**")
        for u, r in failing.iterrows():
            lines.append(f"• `{u}` — {r.clients:,} clients, {_fmt(r.requests)} req")

    # Discord's limit is 2000 characters per message
    chunks, buf = [], ""
    for line in lines:
        if len(buf) + len(line) + 1 > 1900:
            chunks.append(buf)
            buf = ""
        buf += line + "\n"
    if buf:
        chunks.append(buf)

    c = c.assign(success_pct=(100 * c.ok / c.requests).round(1), mb=(c.bytes / 1e6).round(1)).drop(columns=["ok", "bytes"])
    return chunks, c


def run_weekly_report(end_day: date | None = None, days: int = 7, webhook_url: str | None = None, send: bool = True, out_dir: str = "."):
    """Collect, build and (optionally) send the report to Discord. Returns the message chunks."""
    from src.misc.helper_functions import send_discord_message

    end_day = end_day or (datetime.now(timezone.utc).date() - timedelta(days=1))
    cur, prev = collect(end_day, days)
    chunks, clients = build_report(cur, prev, end_day, days)
    csv_path = os.path.join(out_dir, f"api_clients_{end_day.isoformat()}.csv")
    clients.head(500).to_csv(csv_path, index=False)
    for msg in chunks:
        print(msg)
    if send:
        webhook_url = webhook_url or os.getenv("DISCORD_API_USAGE") or os.getenv("DISCORD_ALERTS")
        for i, msg in enumerate(chunks):
            last = i == len(chunks) - 1
            send_discord_message(msg, webhook_url, image_paths=csv_path if last else None, embed_first_image=False)
    return chunks
