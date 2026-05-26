import logging
import math
import os
import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from src.config import gtp_metrics_new


SEO_VERSION = 1
GTP_WEB_BASE_URL = "https://www.growthepie.com"
GTP_API_BASE_URL = "https://api.growthepie.com/v1"
MAX_FACTS = 7

CHAIN_SEO_METRICS = [
    "txcount",
    "daa",
    "fees",
    "throughput",
    "app_revenue",
    "tvl",
    "stables_mcap",
    "profit",
    "market_cap",
]

FUNDAMENTALS_SLUG_TO_METRIC = {
    "daily-active-addresses": "daa",
    "fees-paid-by-users": "fees",
    "stablecoin-market-cap": "stables_mcap",
    "total-value-secured": "tvl",
    "transaction-count": "txcount",
    "transaction-costs": "txcosts",
    "rent-paid": "rent_paid",
    "profit": "profit",
    "fully-diluted-valuation": "fdv",
    "market-cap": "market_cap",
    "throughput": "throughput",
    "app-revenue": "app_revenue",
}

METRIC_DEFINITIONS = {
    "txcount": "the number of transactions processed by each tracked chain",
    "daa": "the number of unique addresses active on each tracked chain each day",
    "fees": "the gas fees paid by users to use each tracked chain",
    "app_revenue": "the application fees captured by tracked applications on each chain",
    "tvl": "the value secured by each tracked chain across its supported assets and applications",
    "stables_mcap": "the stablecoin supply held on each tracked chain",
    "txcosts": "the median transaction cost paid by users on each tracked chain",
    "rent_paid": "the costs paid by chains to Ethereum Mainnet for data availability and settlement",
    "profit": "the difference between chain revenue and settlement or data availability costs",
    "fdv": "the fully diluted valuation of tracked chain tokens",
    "market_cap": "the market capitalization of tracked chain tokens",
    "throughput": "the amount of gas processed per second by each tracked chain",
}

FUNDAMENTALS_METHODOLOGY = {
    "txcount": "Transaction count is calculated from indexed chain activity. For rollups and Layer 2s, growthepie tracks chain-level transactions and normalizes them into comparable daily time series.",
    "daa": "Daily active addresses are calculated from distinct addresses that transact on each tracked chain. Values are normalized into comparable daily chain-level time series.",
    "fees": "Fees paid by users are calculated from gas fees in indexed chain activity and converted to USD and ETH where pricing data is available.",
    "txcosts": "Transaction costs use median user-paid transaction fees from indexed chain activity. Values are normalized so chains can be compared over the same daily periods.",
}

CHAIN_METHODOLOGY = (
    "Chain metrics are derived from growthepie's indexed Ethereum and Layer 2 datasets. "
    "Values are updated daily and may change as labels, source data, and chain coverage improve."
)
FUNDAMENTALS_FALLBACK_METHODOLOGY = (
    "This metric is calculated from growthepie's normalized chain-level datasets. "
    "Values are updated daily and made comparable across tracked Ethereum and Layer 2 networks where possible."
)
APP_METHODOLOGY = (
    "Application metrics are based on growthepie's Open Labels Initiative contract labels and indexed onchain activity. "
    "Values may change as contract labels are added or refined."
)


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def chain_url_slug(origin_key: str) -> str:
    if origin_key == "imx":
        return "immutable-x"
    if origin_key == "rhino":
        return "rhino-fi"
    return origin_key.replace("_", "-")


def safe_title(value: str) -> str:
    return value.replace("-", " ").replace("_", " ").title()


def clean_sentence(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if not value:
        return None
    cleaned = " ".join(str(value).strip().split())
    if not cleaned:
        return None
    return cleaned[:-1] if cleaned.endswith(".") else cleaned


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    cleaned = " ".join(str(value).strip().split())
    return cleaned or None


def chain_intro_sentence(chain: Any) -> str:
    description = clean_sentence(getattr(chain, "metadata_description", None))
    if not description:
        return f"{chain.name} is a blockchain network tracked by growthepie."

    lower_description = description.lower()
    lower_name = chain.name.lower()
    if lower_description.startswith(f"{lower_name} is ") or lower_description == lower_name:
        return f"{description}."
    if lower_description.startswith("is "):
        return f"{chain.name} {description}."
    return f"{chain.name} is {description}."


def concise_app_name(name: Optional[str], owner_project: str) -> str:
    cleaned = clean_text(name) or safe_title(owner_project)
    cleaned = re.sub(r"\s*-\s*Account Abstraction$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r",?\s+(LLC|LLC\.|Inc\.|Inc|Ltd\.|Ltd|GmbH|AG|S\.A\.)$", "", cleaned)
    return cleaned.strip(" ,.") or safe_title(owner_project)


def word_count(text: str) -> int:
    return len([part for part in text.split() if part.strip()])


def format_percent(value: Optional[float]) -> Optional[str]:
    if value is None or not math.isfinite(value):
        return None
    return f"{value:+.1%}"


def format_share_percent(value: Optional[float]) -> Optional[str]:
    if value is None or not math.isfinite(value):
        return None
    if 0 < value < 0.001:
        return "<0.1%"
    return f"{value:.1%}"


def format_value(value: Optional[float], metric_cfg: Optional[Dict[str, Any]] = None) -> Optional[str]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None

    metric_cfg = metric_cfg or {}
    units = metric_cfg.get("units", {})
    is_usd = "usd" in units
    suffix = None
    if "value" in units:
        suffix = units.get("value", {}).get("suffix")

    abs_val = abs(numeric)
    unit_suffix = ""
    scaled = numeric
    decimals = 0
    for threshold, label, dec in [
        (1_000_000_000_000, "T", 2 if is_usd else 1),
        (1_000_000_000, "B", 2 if is_usd else 1),
        (1_000_000, "M", 2 if is_usd else 1),
        (1_000, "K", 1),
    ]:
        if abs_val >= threshold:
            scaled = numeric / threshold
            unit_suffix = label
            decimals = dec
            break
    else:
        configured_decimals = 2 if is_usd else int(units.get("value", {}).get("decimals", 0) or 0)
        if 0 < abs_val < 1:
            decimals = max(configured_decimals, 2)
        elif abs_val < 100 and configured_decimals:
            decimals = min(configured_decimals, 4)

    formatted = f"{scaled:,.{decimals}f}"
    if decimals > 0:
        formatted = formatted.rstrip("0").rstrip(".")
    if formatted == "-0":
        formatted = "0"
    if is_usd:
        if formatted.startswith("-"):
            formatted = f"-${formatted[1:]}"
        else:
            formatted = f"${formatted}"
    formatted = f"{formatted}{unit_suffix}"
    if suffix:
        formatted = f"{formatted} {suffix}"
    return formatted


def change_phrase(value: Optional[float]) -> Optional[str]:
    formatted = format_percent(value)
    if formatted is None:
        return None
    if value > 0:
        return f"up {formatted[1:]}"
    if value < 0:
        return f"down {formatted[1:]}"
    return "unchanged"


def join_names(names: List[str]) -> str:
    names = [name for name in names if name]
    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return f"{', '.join(names[:-1])}, and {names[-1]}"


def pct_change(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous is None:
        return None
    if previous == 0:
        return None
    return round((current - previous) / previous, 4)


def normalize_series_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["date", "value"])
    out = df[["date", "value"]].copy()
    out["date"] = pd.to_datetime(out["date"], utc=True).dt.date
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["value"]).sort_values("date")
    out = out.drop_duplicates(subset=["date"], keep="last")
    return out


def analyze_timeseries(df: pd.DataFrame) -> Dict[str, Any]:
    out = {
        "latest": None,
        "latest_date": None,
        "previous_7d": None,
        "wow_change": None,
        "previous_30d": None,
        "change_30d": None,
        "ath": None,
        "ath_date": None,
        "is_ath": False,
        "high_90d": None,
        "is_90d_high": False,
    }
    df = normalize_series_frame(df)
    if df.empty:
        return out

    latest_row = df.iloc[-1]
    latest = float(latest_row["value"])
    latest_date = latest_row["date"]
    out["latest"] = latest
    out["latest_date"] = latest_date.isoformat()

    date_to_value = dict(zip(df["date"], df["value"]))
    previous_7d = date_to_value.get(latest_date - pd.Timedelta(days=7))
    previous_30d = date_to_value.get(latest_date - pd.Timedelta(days=30))
    out["previous_7d"] = float(previous_7d) if previous_7d is not None else None
    out["previous_30d"] = float(previous_30d) if previous_30d is not None else None
    out["wow_change"] = pct_change(out["latest"], out["previous_7d"])
    out["change_30d"] = pct_change(out["latest"], out["previous_30d"])

    ath_idx = df["value"].idxmax()
    ath_row = df.loc[ath_idx]
    ath = float(ath_row["value"])
    out["ath"] = ath
    out["ath_date"] = ath_row["date"].isoformat()
    out["is_ath"] = math.isclose(latest, ath, rel_tol=0, abs_tol=1e-9)

    window_start = latest_date - pd.Timedelta(days=89)
    df_90d = df[df["date"] >= window_start]
    if not df_90d.empty:
        high_90d = float(df_90d["value"].max())
        out["high_90d"] = high_90d
        out["is_90d_high"] = math.isclose(latest, high_90d, rel_tol=0, abs_tol=1e-9)
    return out


def add_formatted_snapshot_fields(snapshot: Dict[str, Any], metric_cfg: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = dict(snapshot)
    snapshot["latest_formatted"] = format_value(snapshot.get("latest"), metric_cfg)
    snapshot["previous_7d_formatted"] = format_value(snapshot.get("previous_7d"), metric_cfg)
    snapshot["wow_change_formatted"] = format_percent(snapshot.get("wow_change"))
    snapshot["previous_30d_formatted"] = format_value(snapshot.get("previous_30d"), metric_cfg)
    snapshot["change_30d_formatted"] = format_percent(snapshot.get("change_30d"))
    snapshot["ath_formatted"] = format_value(snapshot.get("ath"), metric_cfg)
    snapshot["high_90d_formatted"] = format_value(snapshot.get("high_90d"), metric_cfg)
    return snapshot


def ensure_summary_length(summary: str, fallback_sentences: Iterable[str]) -> str:
    summary = " ".join(summary.split())
    for sentence in fallback_sentences:
        if word_count(summary) >= 80:
            break
        sentence = sentence.strip()
        if sentence and sentence not in summary:
            summary = f"{summary} {sentence}"
    return summary


def primary_metric_key(metric_cfg: Dict[str, Any]) -> str:
    metric_keys = metric_cfg["metric_keys"]
    if "usd" in metric_cfg.get("units", {}):
        usd_keys = [key for key in metric_keys if key.endswith("_usd")]
        if usd_keys:
            return usd_keys[0]
    non_eth_keys = [key for key in metric_keys if not key.endswith("_eth")]
    return non_eth_keys[0] if non_eth_keys else metric_keys[0]


def validate_seo_payload(payload: Dict[str, Any], family: str) -> None:
    for key in ["version", "last_updated_utc", "data"]:
        if key not in payload:
            raise ValueError(f"{family}: missing top-level key '{key}'")
    if payload["version"] != SEO_VERSION:
        raise ValueError(f"{family}: invalid version {payload['version']}")
    if not isinstance(payload["data"], dict):
        raise ValueError(f"{family}: data must be an object")

    required_entry_keys = ["slug", "name", "canonical_url", "title", "summary", "facts", "methodology", "source_urls"]
    forbidden_tokens = ["undefined", "nan", "null"]
    for entry_key, entry in payload["data"].items():
        for required in required_entry_keys:
            if required not in entry:
                raise ValueError(f"{family}/{entry_key}: missing '{required}'")
        if not isinstance(entry["facts"], list) or len(entry["facts"]) < 2:
            raise ValueError(f"{family}/{entry_key}: expected at least 2 facts")
        if not isinstance(entry["source_urls"], list) or not all(isinstance(url, str) for url in entry["source_urls"]):
            raise ValueError(f"{family}/{entry_key}: source_urls must be a list of strings")
        wc = word_count(entry["summary"])
        if wc < 80 or wc > 220:
            raise ValueError(f"{family}/{entry_key}: summary word count {wc} is outside 80-220")
        _validate_values(entry, f"{family}/{entry_key}", forbidden_tokens)


def _validate_values(value: Any, path: str, forbidden_tokens: List[str]) -> None:
    if isinstance(value, str):
        lower = value.lower()
        for token in forbidden_tokens:
            if re.search(rf"(^|[^a-z0-9_]){re.escape(token)}([^a-z0-9_]|$)", lower):
                raise ValueError(f"{path}: string contains forbidden token '{token}'")
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path}: numeric value is not finite")
    elif isinstance(value, dict):
        for key, child in value.items():
            _validate_values(child, f"{path}.{key}", forbidden_tokens)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _validate_values(child, f"{path}[{index}]", forbidden_tokens)


class SeoJsonBuilder:
    def __init__(
        self,
        db_connector: Any,
        main_config: List[Any],
        api_version: str = "v1",
        logger: Optional[logging.Logger] = None,
        alert_func: Optional[Callable[[str], None]] = None,
    ):
        self.db_connector = db_connector
        self.main_config = main_config
        self.api_version = api_version
        self.logger = logger or logging.getLogger(__name__)
        self.alert_func = alert_func
        self.metrics = gtp_metrics_new["chains"]
        self.app_metrics = gtp_metrics_new["apps"]
        self.last_updated_utc = iso_utc_now()
        self.degraded_entries: List[str] = []

    def build_all(self) -> Dict[str, Dict[str, Any]]:
        metric_frames = self._load_chain_metric_frames()
        chain_stats = self._build_chain_metric_stats(metric_frames)
        top_apps_by_chain = self._load_top_apps_by_chain()
        app_metadata = self._load_app_metadata()
        app_activity = self._load_app_activity_7d()

        payloads = {
            "chains": self.build_chains_payload(chain_stats, top_apps_by_chain),
            "fundamentals": self.build_fundamentals_payload(chain_stats),
            "apps": self.build_apps_payload(app_metadata, app_activity),
        }
        for family, payload in payloads.items():
            validate_seo_payload(payload, family)
        self._alert_degraded_entries()
        return payloads

    def build_chains_payload(
        self,
        chain_stats: Dict[str, Dict[str, Dict[str, Any]]],
        top_apps_by_chain: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        data = {}
        for chain in self._production_chains():
            slug = chain_url_slug(chain.origin_key)
            try:
                metrics = {}
                for metric_id in CHAIN_SEO_METRICS:
                    if metric_id in (chain.api_exclude_metrics or []):
                        continue
                    snapshot = chain_stats.get(metric_id, {}).get(chain.origin_key)
                    if snapshot:
                        metrics[metric_id] = snapshot

                top_apps = top_apps_by_chain.get(chain.origin_key, [])
                entry = self._build_chain_entry(chain, slug, metrics, top_apps)
                data[slug] = entry
            except Exception as exc:
                self._degrade(f"chains/{chain.origin_key}", exc)
                data[slug] = self._fallback_chain_entry(chain, slug)

        return {"version": SEO_VERSION, "last_updated_utc": self.last_updated_utc, "data": data}

    def build_fundamentals_payload(self, chain_stats: Dict[str, Dict[str, Dict[str, Any]]]) -> Dict[str, Any]:
        data = {}
        for slug, metric_id in FUNDAMENTALS_SLUG_TO_METRIC.items():
            try:
                metric_cfg = self.metrics[metric_id]
                metric_stats = chain_stats.get(metric_id, {})
                entry = self._build_fundamentals_entry(slug, metric_id, metric_cfg, metric_stats)
                data[slug] = entry
            except Exception as exc:
                self._degrade(f"fundamentals/{slug}", exc)
                data[slug] = self._fallback_fundamentals_entry(slug, metric_id)

        return {"version": SEO_VERSION, "last_updated_utc": self.last_updated_utc, "data": data}

    def build_apps_payload(
        self,
        app_metadata: Dict[str, Dict[str, Any]],
        app_activity: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        data = {}
        for owner_project, metadata in app_metadata.items():
            try:
                activity = app_activity.get(owner_project, {})
                data[owner_project] = self._build_app_entry(owner_project, metadata, activity)
            except Exception as exc:
                self._degrade(f"apps/{owner_project}", exc)
                data[owner_project] = self._fallback_app_entry(owner_project, metadata)

        return {"version": SEO_VERSION, "last_updated_utc": self.last_updated_utc, "data": data}

    def _production_chains(self) -> List[Any]:
        chains = []
        for chain in self.main_config:
            if not chain.api_in_main:
                continue
            if self.api_version != "dev" and chain.api_deployment_flag != "PROD":
                continue
            chains.append(chain)
        return chains

    def _chain_name_map(self) -> Dict[str, str]:
        return {chain.origin_key: chain.name for chain in self.main_config}

    def _chain_slug_map(self) -> Dict[str, str]:
        return {chain.origin_key: chain_url_slug(chain.origin_key) for chain in self.main_config}

    def _load_chain_metric_frames(self) -> Dict[str, pd.DataFrame]:
        origin_keys = [chain.origin_key for chain in self._production_chains()]
        metric_key_to_metric_id = {}
        for metric_id in set(CHAIN_SEO_METRICS + list(FUNDAMENTALS_SLUG_TO_METRIC.values())):
            metric_key_to_metric_id[primary_metric_key(self.metrics[metric_id])] = metric_id

        if not origin_keys or not metric_key_to_metric_id:
            return {}

        origins_sql = ", ".join(f"'{origin}'" for origin in origin_keys)
        metric_keys_sql = ", ".join(f"'{metric_key}'" for metric_key in metric_key_to_metric_id)
        query = f"""
            SELECT origin_key, metric_key, date, value
            FROM fact_kpis
            WHERE origin_key IN ({origins_sql})
                AND metric_key IN ({metric_keys_sql})
                AND date < current_date
            ORDER BY metric_key, origin_key, date
        """
        df = self.db_connector.execute_query(query, load_df=True)
        if df.empty:
            self._degrade("chain_metrics", "no rows returned from fact_kpis")
            return {}

        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df.loc[df["metric_key"] == "gas_per_second", "value"] = df.loc[df["metric_key"] == "gas_per_second", "value"] / 1_000_000

        frames = {}
        for metric_key, metric_id in metric_key_to_metric_id.items():
            frames[metric_id] = df[df["metric_key"] == metric_key].copy()
        return frames

    def _build_chain_metric_stats(self, metric_frames: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, Dict[str, Any]]]:
        all_stats = {}
        for metric_id, df_metric in metric_frames.items():
            metric_cfg = self.metrics[metric_id]
            metric_stats = {}
            if not df_metric.empty:
                for origin_key, group in df_metric.groupby("origin_key"):
                    snapshot = analyze_timeseries(group[["date", "value"]])
                    snapshot = add_formatted_snapshot_fields(snapshot, metric_cfg)
                    snapshot["label"] = metric_cfg["name"]
                    metric_stats[origin_key] = snapshot
                self._add_ranks(metric_stats)
            all_stats[metric_id] = metric_stats
        return all_stats

    def _add_ranks(self, metric_stats: Dict[str, Dict[str, Any]]) -> None:
        ranked = [
            (origin_key, snapshot["latest"])
            for origin_key, snapshot in metric_stats.items()
            if snapshot.get("latest") is not None
        ]
        ranked.sort(key=lambda item: (-item[1], item[0]))
        rank_total = len(ranked)
        for rank, (origin_key, _) in enumerate(ranked, start=1):
            metric_stats[origin_key]["rank"] = rank
            metric_stats[origin_key]["rank_total"] = rank_total

    def _load_top_apps_by_chain(self) -> Dict[str, List[Dict[str, Any]]]:
        chain_keys = [chain.origin_key for chain in self._production_chains() if chain.api_in_apps]
        if not chain_keys:
            return {}
        chains_sql = ", ".join(f"'{chain}'" for chain in chain_keys)
        query = f"""
            WITH ranked AS (
                SELECT
                    fact.origin_key,
                    fact.owner_project,
                    COALESCE(oss.display_name, fact.owner_project) AS display_name,
                    SUM(fact.txcount) AS txcount,
                    ROW_NUMBER() OVER (
                        PARTITION BY fact.origin_key
                        ORDER BY SUM(fact.txcount) DESC, fact.owner_project
                    ) AS rank
                FROM vw_apps_contract_level_materialized fact
                LEFT JOIN oli_oss_directory oss ON fact.owner_project = oss.name
                WHERE fact.date >= current_date - interval '7 days'
                    AND fact.date < current_date
                    AND fact.origin_key IN ({chains_sql})
                    AND fact.owner_project IS NOT NULL
                GROUP BY 1,2,3
            )
            SELECT origin_key, owner_project, display_name, txcount, rank
            FROM ranked
            WHERE rank <= 3
            ORDER BY origin_key, rank
        """
        df = self.db_connector.execute_query(query, load_df=True)
        if df.empty:
            self._degrade("top_apps_by_chain", "no rows returned")
            return {}
        result: Dict[str, List[Dict[str, Any]]] = {}
        for origin_key, group in df.groupby("origin_key"):
            result[origin_key] = [
                {
                    "owner_project": row["owner_project"],
                    "name": clean_text(row["display_name"]) or safe_title(row["owner_project"]),
                    "summary_name": concise_app_name(row["display_name"], row["owner_project"]),
                    "txcount": float(row["txcount"]) if pd.notna(row["txcount"]) else None,
                    "rank": int(row["rank"]),
                }
                for _, row in group.iterrows()
            ]
        return result

    def _load_app_metadata(self) -> Dict[str, Dict[str, Any]]:
        try:
            chain_keys = [chain.origin_key for chain in self.main_config if chain.api_in_apps]
            df = self.db_connector.get_active_projects(add_category=True, filtered_by_chains=chain_keys)
        except Exception as exc:
            self._degrade("app_metadata", exc)
            return {}
        if df.empty:
            self._degrade("app_metadata", "no active projects returned")
            return {}

        result = {}
        for _, row in df.iterrows():
            owner_project = row.get("name")
            if not owner_project:
                continue
            result[owner_project] = row.to_dict()
        return result

    def _load_app_activity_7d(self) -> Dict[str, Dict[str, Any]]:
        chain_keys = [chain.origin_key for chain in self.main_config if chain.api_in_apps]
        if not chain_keys:
            return {}
        chains_sql = ", ".join(f"'{chain}'" for chain in chain_keys)
        query = f"""
            WITH apps_mat AS (
                SELECT
                    fact.owner_project,
                    fact.origin_key,
                    COUNT(DISTINCT fact.address) AS num_contracts,
                    COALESCE(SUM(fact.fees_paid_usd) FILTER (WHERE fact.date > current_date - interval '8 days'), 0) AS gas_fees_usd,
                    COALESCE(SUM(fact.txcount) FILTER (WHERE fact.date > current_date - interval '8 days'), 0) AS txcount,
                    COALESCE(SUM(fact.txcount) FILTER (WHERE fact.date <= current_date - interval '8 days'), 0) AS prev_txcount
                FROM vw_apps_contract_level_materialized fact
                WHERE fact.date >= current_date - interval '14 days'
                    AND fact.date < current_date
                    AND fact.origin_key IN ({chains_sql})
                    AND fact.owner_project IS NOT NULL
                GROUP BY 1,2
            ),
            daa AS (
                SELECT
                    oli.owner_project,
                    fact.origin_key,
                    COALESCE(hll_cardinality(hll_union_agg(CASE WHEN fact.date > current_date - interval '8 days' THEN hll_addresses END))::int, 0) AS daa,
                    COALESCE(hll_cardinality(hll_union_agg(CASE WHEN fact.date <= current_date - interval '8 days' THEN hll_addresses END))::int, 0) AS prev_daa
                FROM public.fact_active_addresses_contract_hll fact
                JOIN vw_oli_label_pool_gold_pivoted_v2 oli USING (address, origin_key)
                WHERE fact.date >= current_date - interval '14 days'
                    AND fact.date < current_date
                    AND fact.origin_key IN ({chains_sql})
                    AND oli.owner_project IS NOT NULL
                GROUP BY 1,2
            )
            SELECT
                apps_mat.*,
                COALESCE(daa.daa, 0) AS daa,
                COALESCE(daa.prev_daa, 0) AS prev_daa
            FROM apps_mat
            LEFT JOIN daa USING (owner_project, origin_key)
        """
        df = self.db_connector.execute_query(query, load_df=True)
        if df.empty:
            self._degrade("app_activity_7d", "no rows returned")
            return {}

        result = {}
        chain_names = self._chain_name_map()
        for owner_project, group in df.groupby("owner_project"):
            total_txcount = float(group["txcount"].sum())
            prev_txcount = float(group["prev_txcount"].sum())
            total_daa = float(group["daa"].sum())
            gas_fees_usd = float(group["gas_fees_usd"].sum())
            top_chains = group.sort_values(["txcount", "origin_key"], ascending=[False, True]).head(3)
            result[owner_project] = {
                "txcount_7d": total_txcount,
                "prev_txcount_7d": prev_txcount,
                "txcount_wow_change": pct_change(total_txcount, prev_txcount),
                "daa_7d": total_daa,
                "gas_fees_usd_7d": gas_fees_usd,
                "num_contracts": int(group["num_contracts"].sum()),
                "top_chains": [
                    {
                        "chain_key": row["origin_key"],
                        "name": chain_names.get(row["origin_key"], safe_title(row["origin_key"])),
                        "txcount_7d": float(row["txcount"]),
                    }
                    for _, row in top_chains.iterrows()
                    if row["txcount"] and row["txcount"] > 0
                ],
            }

        total_gas_fees_usd = sum(activity["gas_fees_usd_7d"] for activity in result.values() if activity["gas_fees_usd_7d"] > 0)
        ranked_by_gas = sorted(
            (
                (owner_project, activity["gas_fees_usd_7d"])
                for owner_project, activity in result.items()
                if activity["gas_fees_usd_7d"] > 0
            ),
            key=lambda item: (-item[1], item[0]),
        )
        gas_rank_total = len(ranked_by_gas)
        for rank, (owner_project, gas_fees_usd) in enumerate(ranked_by_gas, start=1):
            result[owner_project]["gas_fees_rank"] = rank
            result[owner_project]["gas_fees_rank_total"] = gas_rank_total
            result[owner_project]["gas_fees_share_7d"] = gas_fees_usd / total_gas_fees_usd if total_gas_fees_usd > 0 else None
            result[owner_project]["gas_fees_share_7d_formatted"] = format_share_percent(result[owner_project]["gas_fees_share_7d"])
        return result

    def _build_chain_entry(
        self,
        chain: Any,
        slug: str,
        metrics: Dict[str, Dict[str, Any]],
        top_apps: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        tx = metrics.get("txcount", {})
        daa = metrics.get("daa", {})
        fees = metrics.get("fees", {})
        stables = metrics.get("stables_mcap", {})
        intro = chain_intro_sentence(chain)

        metric_parts = []
        if tx.get("latest_formatted"):
            metric_parts.append(f"{tx['latest_formatted']} transactions")
        if daa.get("latest_formatted"):
            metric_parts.append(f"{daa['latest_formatted']} daily active addresses")
        latest_sentence = (
            f"In the latest available daily data, {chain.name} recorded {join_names(metric_parts)}."
            if metric_parts
            else f"In the latest available daily data, growthepie tracks chain-level activity for {chain.name}."
        )

        rank_sentence = ""
        tx_phrase = change_phrase(tx.get("wow_change"))
        if tx.get("rank"):
            if tx_phrase:
                rank_sentence = (
                    f"Transaction count was {tx_phrase} week over week, ranking {chain.name} "
                    f"#{tx['rank']} among {tx['rank_total']} tracked chains by transactions."
                )
            else:
                rank_sentence = (
                    f"{chain.name} ranked #{tx['rank']} among {tx['rank_total']} tracked chains by latest daily transactions."
                )

        stables_sentence = self._metric_development_sentence(
            chain.name,
            stables,
            "Stablecoin supply",
            "stablecoin supply",
        )
        revenue_sentence = self._metric_development_sentence(
            chain.name,
            fees,
            "Daily chain revenue",
            "daily chain revenue",
        )

        notable = []
        for metric_id, label in [("txcount", "transaction count"), ("daa", "daily active addresses"), ("stables_mcap", "stablecoin supply")]:
            snapshot = metrics.get(metric_id, {})
            if snapshot.get("is_ath"):
                notable.append(f"{label} reached an all-time high")
            elif snapshot.get("is_90d_high"):
                notable.append(f"{label} reached a 90-day high")
        notable_sentence = f"Notably, {join_names(notable)}." if notable else ""

        apps_sentence = ""
        if top_apps:
            top_app_names = [app.get("summary_name") or app["name"] for app in top_apps]
            apps_sentence = f"Among labeled applications, the highest 7-day transaction activity came from {join_names(top_app_names)}."

        extra_sentence = "Data is updated daily from growthepie's indexed chain, application, and label datasets."
        summary = " ".join(part for part in [intro, latest_sentence, rank_sentence, stables_sentence, revenue_sentence, notable_sentence, apps_sentence, extra_sentence] if part)
        summary = ensure_summary_length(summary, [
            "Current snapshots include activity, usage, value, fee, and application metrics where each dataset is available for the chain.",
            "Missing or incomplete source metrics are omitted from the narrative rather than replaced with generated estimates.",
        ])

        facts = []
        self._append_metric_fact(facts, "Daily transactions", tx)
        self._append_metric_fact(facts, "Daily active addresses", daa)
        self._append_metric_fact(facts, "Fees paid by users", fees)
        self._append_metric_fact(facts, "Stablecoin supply", stables)
        if tx.get("rank"):
            facts.append(f"Transaction count rank: #{tx['rank']} of {tx['rank_total']} tracked chains.")
        if top_apps:
            facts.append(f"Top applications by 7-day transaction count: {join_names([app.get('summary_name') or app['name'] for app in top_apps])}.")
        while len(facts) < 2:
            facts.append("growthepie tracks this chain in its normalized chain-level datasets.")

        source_urls = [f"{GTP_API_BASE_URL}/master.json"]
        for metric_id in metrics:
            source_urls.append(f"{GTP_API_BASE_URL}/metrics/chains/{chain.origin_key}/{metric_id}.json")

        return {
            "slug": slug,
            "chain_key": chain.origin_key,
            "name": chain.name,
            "canonical_url": f"{GTP_WEB_BASE_URL}/chains/{slug}",
            "title": f"{chain.name} chain analytics",
            "summary": summary,
            "facts": facts[:MAX_FACTS],
            "methodology": CHAIN_METHODOLOGY,
            "metrics": metrics,
            "top_applications": top_apps,
            "source_urls": source_urls,
        }

    def _fallback_chain_entry(self, chain: Any, slug: str) -> Dict[str, Any]:
        summary = ensure_summary_length(
            f"{chain.name} is a blockchain network tracked by growthepie. "
            "The current backend run did not find enough complete daily observations to calculate metric snapshots, ranks, changes, highs, or top applications for this chain. "
            "The entry is still emitted so the chain route can render deterministic crawler text without estimated activity numbers.",
            [
                "Future runs will include current measurements when indexed source datasets are available for the chain.",
                "Missing measurements are omitted from facts rather than filled with placeholders.",
            ],
        )
        return {
            "slug": slug,
            "chain_key": chain.origin_key,
            "name": chain.name,
            "canonical_url": f"{GTP_WEB_BASE_URL}/chains/{slug}",
            "title": f"{chain.name} chain analytics",
            "summary": summary,
            "facts": [
                "growthepie tracks this chain in its chain-level datasets.",
                "Current metric snapshots are unavailable for this backend run.",
            ],
            "methodology": CHAIN_METHODOLOGY,
            "metrics": {},
            "source_urls": [f"{GTP_API_BASE_URL}/master.json"],
        }

    def _append_metric_fact(self, facts: List[str], label: str, snapshot: Dict[str, Any]) -> None:
        if not snapshot.get("latest_formatted"):
            return
        sentence = f"{label}: {snapshot['latest_formatted']}"
        change = change_phrase(snapshot.get("wow_change"))
        if change:
            sentence = f"{sentence}, {change} week over week"
        facts.append(f"{sentence}.")

    def _metric_development_sentence(self, chain_name: str, snapshot: Dict[str, Any], label: str, rank_label: str) -> str:
        if not snapshot.get("latest_formatted"):
            return ""

        sentence = f"{label} was {snapshot['latest_formatted']}"
        change = change_phrase(snapshot.get("wow_change"))
        if change:
            sentence = f"{sentence}, {change} week over week"
        if snapshot.get("rank") and snapshot.get("rank_total"):
            sentence = f"{sentence}, ranking {chain_name} #{snapshot['rank']} among {snapshot['rank_total']} tracked chains by {rank_label}"
        return f"{sentence}."

    def _build_fundamentals_entry(
        self,
        slug: str,
        metric_id: str,
        metric_cfg: Dict[str, Any],
        metric_stats: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        chain_names = self._chain_name_map()
        chain_slugs = self._chain_slug_map()
        ranked = [
            (origin_key, snapshot)
            for origin_key, snapshot in metric_stats.items()
            if snapshot.get("latest") is not None
        ]
        ranked.sort(key=lambda item: (-item[1]["latest"], item[0]))
        top = ranked[:3]
        top_chains = [
            {
                "chain_key": origin_key,
                "slug": chain_slugs.get(origin_key, chain_url_slug(origin_key)),
                "name": chain_names.get(origin_key, safe_title(origin_key)),
                "value": snapshot["latest"],
                "value_formatted": snapshot["latest_formatted"],
                "latest_date": snapshot["latest_date"],
                "rank": index,
                "wow_change": snapshot.get("wow_change"),
                "wow_change_formatted": snapshot.get("wow_change_formatted"),
                "is_ath": snapshot.get("is_ath"),
                "is_90d_high": snapshot.get("is_90d_high"),
            }
            for index, (origin_key, snapshot) in enumerate(top, start=1)
        ]

        total = None
        total_formatted = None
        if metric_cfg.get("all_l2s_aggregate") == "sum":
            values = [snapshot["latest"] for _, snapshot in ranked if snapshot.get("latest") is not None]
            total = float(sum(values)) if values else None
            total_formatted = format_value(total, metric_cfg)

        top_names = [item["name"] for item in top_chains]
        definition = METRIC_DEFINITIONS.get(metric_id, "a normalized growthepie chain-level metric")
        if top_names:
            top_sentence = f"In the latest available daily data, the top chains were {join_names(top_names)}."
        else:
            top_sentence = "The page compares tracked chains across the Ethereum ecosystem."
        total_sentence = f"Across tracked chains, the ecosystem total was {total_formatted}." if total_formatted else ""

        notable = ""
        if top_chains:
            leader = top_chains[0]
            if leader.get("is_ath"):
                notable = f"{leader['name']} ranked #1 and reached a new all-time high."
            elif leader.get("is_90d_high"):
                notable = f"{leader['name']} ranked #1 and reached a new 90-day high."
            elif leader.get("wow_change_formatted"):
                notable = f"{leader['name']} ranked #1 with a week-over-week change of {leader['wow_change_formatted']}."

        summary = (
            f"{metric_cfg['name']} measures {definition}. {top_sentence} {total_sentence} "
            f"{notable} Data is updated daily and normalized into comparable chain-level time series."
        )
        summary = ensure_summary_length(summary, [
            "The summary uses the latest available daily observations from backend datasets and avoids substituting estimates when a chain has incomplete data.",
            "Ranks are calculated across production chains that have a current value for the metric.",
        ])

        facts = []
        if top_chains:
            facts.append(f"Top chains by latest daily {metric_cfg['name'].lower()}: {join_names(top_names)}.")
        if total_formatted:
            facts.append(f"Tracked ecosystem total: {total_formatted}.")
        if top_chains:
            leader = top_chains[0]
            facts.append(f"{leader['name']} ranked #1 with {leader['value_formatted']}.")
            if leader.get("is_ath"):
                facts.append(f"{leader['name']} reached an all-time high.")
            elif leader.get("is_90d_high"):
                facts.append(f"{leader['name']} reached a 90-day high.")
        while len(facts) < 2:
            facts.append("growthepie updates this metric after daily source datasets are refreshed.")

        return {
            "slug": slug,
            "metric_key": metric_id,
            "name": metric_cfg["name"],
            "canonical_url": f"{GTP_WEB_BASE_URL}/fundamentals/{slug}",
            "title": f"Ethereum ecosystem {metric_cfg['name'].lower()}",
            "summary": summary,
            "facts": facts[:MAX_FACTS],
            "methodology": FUNDAMENTALS_METHODOLOGY.get(metric_id, FUNDAMENTALS_FALLBACK_METHODOLOGY),
            "top_chains": top_chains,
            "ecosystem_total": total,
            "ecosystem_total_formatted": total_formatted,
            "source_urls": [f"{GTP_API_BASE_URL}/metrics/{metric_id}.json"],
        }

    def _fallback_fundamentals_entry(self, slug: str, metric_id: str) -> Dict[str, Any]:
        metric_cfg = self.metrics.get(metric_id, {"name": safe_title(slug)})
        summary = ensure_summary_length(
            f"{metric_cfg['name']} is a growthepie fundamentals metric for comparing tracked Ethereum and Layer 2 networks. "
            "The current backend run did not find enough complete daily observations to calculate ranks, changes, or highs for this metric. "
            "The entry is still emitted so frontend routes can render deterministic crawler text without generated numerical estimates.",
            [
                "Values are refreshed when normalized source datasets become available, and missing metrics are left out rather than filled with placeholders.",
                "This keeps the JSON shape stable for server rendering while preserving factual accuracy.",
            ],
        )
        return {
            "slug": slug,
            "metric_key": metric_id,
            "name": metric_cfg["name"],
            "canonical_url": f"{GTP_WEB_BASE_URL}/fundamentals/{slug}",
            "title": f"Ethereum ecosystem {metric_cfg['name'].lower()}",
            "summary": summary,
            "facts": [
                "This metric is part of growthepie's fundamentals dataset.",
                "Current ranks and changes are unavailable for this backend run.",
            ],
            "methodology": FUNDAMENTALS_METHODOLOGY.get(metric_id, FUNDAMENTALS_FALLBACK_METHODOLOGY),
            "top_chains": [],
            "source_urls": [f"{GTP_API_BASE_URL}/metrics/{metric_id}.json"],
        }

    def _build_app_entry(self, owner_project: str, metadata: Dict[str, Any], activity: Dict[str, Any]) -> Dict[str, Any]:
        name = clean_text(metadata.get("display_name")) or safe_title(owner_project)
        main_category = clean_text(metadata.get("main_category"))
        sub_category = clean_text(metadata.get("sub_category"))
        category_phrase = self._category_phrase(main_category, sub_category)
        top_chains = activity.get("top_chains", [])
        top_chain_names = [chain["name"] for chain in top_chains]
        tx_change = activity.get("txcount_wow_change")
        tx_change_phrase = change_phrase(tx_change)
        gas_fees_formatted = format_value(activity.get("gas_fees_usd_7d"), self.app_metrics["gas_fees"])
        gas_fees_share_formatted = activity.get("gas_fees_share_7d_formatted")

        top_chains_sentence = (
            f"In the latest 7-day window, its highest activity came from {join_names(top_chain_names)}."
            if top_chain_names
            else "The latest 7-day activity window did not contain enough chain-level activity to identify leading chains."
        )
        metric_sentence = ""
        if activity.get("txcount_7d") is not None:
            metric_sentence = f"Transactions totaled {format_value(activity.get('txcount_7d'), self.app_metrics['txcount'])} over that window"
            if tx_change_phrase:
                metric_sentence = f"{metric_sentence}, {tx_change_phrase} versus the previous 7 days"
            if activity.get("daa_7d") is not None:
                metric_sentence = f"{metric_sentence}, while active addresses reached {format_value(activity.get('daa_7d'), self.app_metrics['daa'])}"
            metric_sentence = f"{metric_sentence}."

        gas_fees_sentence = ""
        if gas_fees_formatted:
            gas_fees_sentence = f"Users paid {gas_fees_formatted} in gas fees to use {name} over the same 7-day period"
            if activity.get("gas_fees_rank") and activity.get("gas_fees_rank_total"):
                gas_fees_sentence = (
                    f"{gas_fees_sentence}, ranking it #{activity['gas_fees_rank']} of "
                    f"{activity['gas_fees_rank_total']} tracked applications by gas fees paid"
                )
            if gas_fees_share_formatted:
                gas_fees_sentence = f"{gas_fees_sentence} and representing {gas_fees_share_formatted} of total tracked application gas fees"
            gas_fees_sentence = f"{gas_fees_sentence}."

        summary = (
            f"{name} is {category_phrase}. On growthepie, its activity is tracked across Ethereum and supported Layer 2 chains using labeled smart contracts. "
            f"{top_chains_sentence} {metric_sentence} {gas_fees_sentence} Data is updated daily and may change as labels are improved."
        )
        summary = ensure_summary_length(summary, [
            "The application summary is generated from backend label, contract, and activity datasets rather than from inferred or model-generated numbers.",
            "When activity is incomplete, the JSON keeps descriptive route metadata and omits unavailable measurements.",
        ])

        facts = []
        if main_category or sub_category:
            facts.append(f"Category: {join_names([main_category, sub_category])}.")
        else:
            facts.append("Category metadata is tracked in the Open Labels Initiative directory.")
        if top_chain_names:
            facts.append(f"Top chains by 7-day activity: {join_names(top_chain_names)}.")
        if activity.get("txcount_7d") is not None:
            tx_fact = f"7-day transactions: {format_value(activity.get('txcount_7d'), self.app_metrics['txcount'])}"
            if tx_change_phrase:
                tx_fact = f"{tx_fact}, {tx_change_phrase} versus the previous 7 days"
            facts.append(f"{tx_fact}.")
        if activity.get("daa_7d") is not None:
            facts.append(f"7-day active addresses: {format_value(activity.get('daa_7d'), self.app_metrics['daa'])}.")
        if gas_fees_formatted:
            gas_fact = f"7-day gas fees paid: {gas_fees_formatted}"
            if activity.get("gas_fees_rank") and activity.get("gas_fees_rank_total"):
                gas_fact = f"{gas_fact}, rank #{activity['gas_fees_rank']} of {activity['gas_fees_rank_total']} tracked applications"
            if gas_fees_share_formatted:
                gas_fact = f"{gas_fact}, {gas_fees_share_formatted} of tracked application gas fees"
            facts.append(f"{gas_fact}.")
        if activity.get("num_contracts") is not None:
            facts.append(f"Tracked contracts with recent activity: {activity['num_contracts']}.")
        while len(facts) < 2:
            facts.append("Application activity is derived from labeled smart contracts.")

        return {
            "slug": owner_project,
            "owner_project": owner_project,
            "name": name,
            "canonical_url": f"{GTP_WEB_BASE_URL}/applications/{owner_project}",
            "title": f"{name} application analytics",
            "summary": summary,
            "facts": facts[:MAX_FACTS],
            "methodology": APP_METHODOLOGY,
            "metrics": {
                "txcount_7d": activity.get("txcount_7d"),
                "txcount_7d_formatted": format_value(activity.get("txcount_7d"), self.app_metrics["txcount"]),
                "txcount_wow_change": tx_change,
                "txcount_wow_change_formatted": format_percent(tx_change),
                "daa_7d": activity.get("daa_7d"),
                "daa_7d_formatted": format_value(activity.get("daa_7d"), self.app_metrics["daa"]),
                "gas_fees_usd_7d": activity.get("gas_fees_usd_7d"),
                "gas_fees_usd_7d_formatted": format_value(activity.get("gas_fees_usd_7d"), self.app_metrics["gas_fees"]),
                "gas_fees_rank": activity.get("gas_fees_rank"),
                "gas_fees_rank_total": activity.get("gas_fees_rank_total"),
                "gas_fees_share_7d": activity.get("gas_fees_share_7d"),
                "gas_fees_share_7d_formatted": gas_fees_share_formatted,
            },
            "top_chains": top_chains,
            "source_urls": [
                f"{GTP_API_BASE_URL}/apps/details/{owner_project}.json",
                f"{GTP_API_BASE_URL}/apps/app_overview_7d.json",
                f"{GTP_API_BASE_URL}/labels/projects_filtered.json",
            ],
        }

    def _fallback_app_entry(self, owner_project: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        name = clean_text(metadata.get("display_name")) or safe_title(owner_project)
        summary = ensure_summary_length(
            f"{name} is an application tracked by growthepie through Open Labels Initiative metadata and labeled smart contracts. "
            "The current backend run did not find enough complete recent activity to calculate top chains, changes, or metric snapshots for this application. "
            "The entry is still emitted so the application route can render deterministic crawler text without estimated activity numbers.",
            [
                "Future runs will include current activity when indexed contract data is available for supported chains.",
                "Missing measurements are omitted from facts rather than replaced with placeholders.",
            ],
        )
        return {
            "slug": owner_project,
            "owner_project": owner_project,
            "name": name,
            "canonical_url": f"{GTP_WEB_BASE_URL}/applications/{owner_project}",
            "title": f"{name} application analytics",
            "summary": summary,
            "facts": [
                "Application metadata is derived from the Open Labels Initiative directory.",
                "Current recent activity metrics are unavailable for this backend run.",
            ],
            "methodology": APP_METHODOLOGY,
            "source_urls": [
                f"{GTP_API_BASE_URL}/apps/details/{owner_project}.json",
                f"{GTP_API_BASE_URL}/labels/projects_filtered.json",
            ],
        }

    def _category_phrase(self, main_category: Optional[str], sub_category: Optional[str]) -> str:
        names = [name for name in [main_category, sub_category] if name]
        if not names:
            return "an application tracked by growthepie"
        if len(names) == 1:
            return f"a {names[0]} application"
        return f"a {names[0]} application categorized as {names[1]}"

    def _degrade(self, key: str, exc: Any) -> None:
        message = f"{key}: {exc}"
        self.degraded_entries.append(message)
        self.logger.warning("SEO JSON degraded entry: %s", message)

    def _alert_degraded_entries(self) -> None:
        if not self.degraded_entries or self.alert_func is None:
            return
        try:
            sample = "\n".join(self.degraded_entries[:20])
            self.alert_func(f"[seo-json] Generated with {len(self.degraded_entries)} degraded entries:\n{sample}")
        except Exception as exc:
            self.logger.warning("Failed to send SEO JSON degraded-entry alert: %s", exc)


def atomic_write_json(path_without_extension: str, payload: Dict[str, Any], dump_func: Callable[[Any, Any], None]) -> None:
    full_path = f"output/{path_without_extension}.json"
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    tmp_path = f"{full_path}.tmp"
    with open(tmp_path, "w") as fp:
        dump_func(payload, fp)
    os.replace(tmp_path, full_path)
