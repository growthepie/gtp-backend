"""Find days where a chain metric breaks far out of its recent range.

Flow metrics (counts and fees per day) are compared with the median of the
previous BASELINE_DAYS days. Stock metrics (balances) are compared with the
day before, because their level drifts and a median says little about a jump.
"""
from dataclasses import dataclass, asdict

import numpy as np
import pandas as pd

BASELINE_DAYS = 28

# metric_key -> how to judge it. min_value / min_change keep tiny chains from triggering.
SPIKE_METRICS = {
    "fees_paid_usd":      {"kind": "flow",  "label": "Fees paid",          "min_ratio": 4.0, "min_value": 250_000},
    "txcount":            {"kind": "flow",  "label": "Transactions",       "min_ratio": 3.0, "min_value": 1_000_000},
    "daa":                {"kind": "flow",  "label": "Active addresses",   "min_ratio": 3.0, "min_value": 200_000},
    "txcosts_median_usd": {"kind": "flow",  "label": "Median tx cost",     "min_ratio": 5.0, "min_value": 0.05},
    "gas_per_second":     {"kind": "flow",  "label": "Throughput",         "min_ratio": 3.0, "min_value": 5_000_000},
    "app_fees_usd":       {"kind": "flow",  "label": "App revenue",        "min_ratio": 4.0, "min_value": 500_000},
    "stables_mcap":       {"kind": "stock", "label": "Stablecoin supply",  "min_pct": 0.20, "min_change": 100_000_000},
    "tvl":                {"kind": "stock", "label": "Value secured",      "min_pct": 0.20, "min_change": 250_000_000},
}

MIN_ROBUST_Z = 6.0
# A spike that runs over several days is one story: skip a day if the same chain and
# metric already had an event within this many days before it.
SUPPRESS_DAYS = 3


@dataclass
class Spike:
    origin_key: str
    metric_key: str
    date: str
    value: float
    baseline_median: float
    ratio: float
    robust_z: float
    score: float

    def to_dict(self) -> dict:
        return asdict(self)


def _robust_z(value: float, baseline: np.ndarray) -> tuple[float, float]:
    median = float(np.median(baseline))
    mad = float(np.median(np.abs(baseline - median)))
    scale = 1.4826 * mad if mad > 0 else max(abs(median) * 0.01, 1e-9)
    return median, (value - median) / scale


def find_spikes(df: pd.DataFrame, check_dates: list[str], metrics: dict = SPIKE_METRICS) -> list[Spike]:
    """df has columns origin_key, metric_key, date, value. Returns spikes on check_dates, highest score first."""
    if df.empty:
        return []
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    spikes = []
    for (origin_key, metric_key), g in df.groupby(["origin_key", "metric_key"]):
        conf = metrics.get(metric_key)
        if conf is None:
            continue
        s = g.set_index("date")["value"].astype(float).sort_index()
        for day in check_dates:
            if day not in s.index:
                continue
            value = s[day]
            history = s[s.index < day].tail(BASELINE_DAYS).to_numpy()
            if len(history) < BASELINE_DAYS // 2 or np.isnan(value):
                continue
            median, z = _robust_z(value, history)

            if conf["kind"] == "flow":
                if median <= 0 or value < conf["min_value"]:
                    continue
                ratio = value / median
                if ratio < conf["min_ratio"] or z < MIN_ROBUST_Z:
                    continue
                score = float(np.log(ratio))
            else:
                prev = history[-1]
                if prev <= 0:
                    continue
                change = value - prev
                ratio = value / prev
                if abs(ratio - 1) < conf["min_pct"] or abs(change) < conf["min_change"] or abs(z) < MIN_ROBUST_Z:
                    continue
                score = float(abs(np.log(ratio))) * 2  # a 20% balance move is as notable as a much larger flow spike

            spikes.append(Spike(origin_key, metric_key, day, float(value), median, float(ratio), float(z), score))
    return sorted(spikes, key=lambda x: x.score, reverse=True)


def drop_known(spikes: list[Spike], events: pd.DataFrame) -> tuple[list[Spike], list[Spike]]:
    """Split spikes into (new, suppressed) using past events: exact repeats and follow-up days of a recent event.

    Spikes in this batch count as events too, so day 2 of a two-day spike is suppressed in the same run.
    """
    known = [] if events is None or events.empty else [
        (r.origin_key, r.metric_key, pd.Timestamp(r.date)) for r in events.itertuples()
    ]
    new, suppressed = [], []
    for sp in sorted(spikes, key=lambda x: x.date):
        day = pd.Timestamp(sp.date)
        same = [d for ok, mk, d in known if ok == sp.origin_key and mk == sp.metric_key]
        if day in same:
            continue  # already handled
        recent = [d for d in same if day - pd.Timedelta(days=SUPPRESS_DAYS) <= d < day]
        (suppressed if recent else new).append(sp)
        known.append((sp.origin_key, sp.metric_key, day))
    new.sort(key=lambda x: x.score, reverse=True)
    return new, suppressed


def group_by_event(spikes: list[Spike]) -> list[list[Spike]]:
    """Several metrics of one chain spiking on the same day are one story. Groups keep the score order."""
    groups: dict[tuple, list[Spike]] = {}
    for sp in spikes:
        groups.setdefault((sp.origin_key, sp.date), []).append(sp)
    return list(groups.values())
