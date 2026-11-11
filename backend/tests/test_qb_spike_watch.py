"""Offline checks for the quick bite spike watch: detection on the Oct 29, 2026 Ethereum fee spike, and rendering.

Run with: python -m tests.test_qb_spike_watch  (from backend/)
"""
import json

import pandas as pd

from src.quick_bites.spike_watch.detect import drop_known, find_spikes, group_by_event
from src.quick_bites.spike_watch.render import chart_jsons, quick_bite_ts, register_quick_bite, slugify, unique_slug, var_name

# Ethereum fees_paid_usd from growthepie, Oct 15 - Nov 5, 2026
FEES = [465183, 294600, 228834, 198989, 264503, 409897, 340135, 420986, 249152, 259528, 171057, 271756, 267955,
        259190, 6989699, 4579362, 355415, 171822, 269883, 275200, 404678, 771586]
TXCOUNT = [1857595, 1770089, 1563170, 1595893, 1748408, 1837508, 1883680, 1812311, 1886159, 1721629, 1540610, 1790498,
           1889143, 1996082, 1761581, 1881441, 1627024, 1660042, 1884882, 1895037, 1990008, 2189262]


def _df():
    days = pd.date_range("2026-10-15", periods=len(FEES)).strftime("%Y-%m-%d")
    rows = [("ethereum", "fees_paid_usd", d, v) for d, v in zip(days, FEES)]
    rows += [("ethereum", "txcount", d, v) for d, v in zip(days, TXCOUNT)]
    rows += [("tinychain", "fees_paid_usd", d, 10 * (100 if d == "2026-10-29" else 1)) for d in days]
    return pd.DataFrame(rows, columns=["origin_key", "metric_key", "date", "value"])


def test_detects_fee_spike_only():
    spikes = find_spikes(_df(), ["2026-10-28", "2026-10-29"])
    assert [(s.origin_key, s.metric_key, s.date) for s in spikes] == [("ethereum", "fees_paid_usd", "2026-10-29")]
    assert spikes[0].ratio > 20


def test_second_day_suppressed_in_same_run_and_later():
    spikes = find_spikes(_df(), ["2026-10-29", "2026-10-30"])
    assert {s.date for s in spikes} == {"2026-10-29", "2026-10-30"}
    new, suppressed = drop_known(spikes, pd.DataFrame())
    assert [s.date for s in new] == ["2026-10-29"] and [s.date for s in suppressed] == ["2026-10-30"]

    events = pd.DataFrame([{"origin_key": "ethereum", "metric_key": "fees_paid_usd", "date": "2026-10-29"}])
    new, suppressed = drop_known(find_spikes(_df(), ["2026-10-29", "2026-10-30"]), events)
    assert new == [] and [s.date for s in suppressed] == ["2026-10-30"]
    assert len(group_by_event(spikes)) == 2


def test_quiet_days_have_no_spikes():
    assert find_spikes(_df(), ["2026-11-02", "2026-11-03"]) == []


SPEC = {
    "newsworthy": True, "skip_reason": "", "headline": "Bots raced after the Binance hack.", "confidence": "high",
    "slug": "Ethereum fee spike 2026-10-29!", "title": "Why Ethereum made $11M in fees in one night",
    "subtitle": "A hack, price gaps and a bot race", "short_title": "Oct 29 fee spike, a long title",
    "summary": "Fees jumped 26x.",
    "blocks": [
        {"kind": "heading", "text": "What happened"},
        {"kind": "paragraph", "text": "Fees rose to **$6.99M** on Oct 29."},
        {"kind": "kpis", "kpis": [{"title": "Fees on Oct 29", "value": "$6.99M"}]},
        {"kind": "chart", "chart": {"title": "Fees per hour", "chart_type": "column", "series": [
            {"data_name": "hourly_fees", "y_index": 1, "label": "Fees", "prefix": "$"},
            {"data_name": "missing", "y_index": 1, "label": "Ghost"}]}},
        {"kind": "bullets", "items": ["one", "two"]},
    ],
    "sources": [{"title": "TechCrunch", "url": "https://techcrunch.com/x", "used_for": "hack"}],
    "unverified_claims": [], "method_note": "growthepie hourly fees.",
}
CHARTS = {"hourly_fees": {"description": "fees per hour", "types": ["unix", "fees_usd"],
                          "values": [[1761768000000, 9043.4], [1761771600000, 1123096.7]]}}


def test_render_quick_bite():
    slug = unique_slug(slugify(SPEC["slug"]), {"ethereum-fee-spike-2026-10-29"})
    assert slug == "ethereum-fee-spike-2026-10-29-2"
    ts, warnings = quick_bite_ts(SPEC, slug, CHARTS, "Ethereum Mainnet", "ethereum", "#94ABD3")
    assert f"const {var_name(slug)}: QuickBiteData" in ts
    assert '"shortTitle": ' not in ts and 'shortTitle: "Oct 29 fee spike, a"' in ts  # capped at 20 chars
    assert "https://api.growthepie.com/v1/quick-bites/spikes/ethereum-fee-spike-2026-10-29-2/hourly_fees.json" in ts
    assert len(warnings) == 1 and "Ghost" in warnings[0]
    assert '"# Sources"' in ts and "growthepie_eth" in ts
    body = chart_jsons(CHARTS)["hourly_fees"]
    assert body["data"]["series"]["values"][1][1] == 1123096.7 and json.dumps(body)


def test_register_quick_bite():
    index_ts = (
        "import { QuickBiteData } from '@/lib/types/quickBites';\nimport gloDollar from './qb-glo-dollar';\n\n"
        "const QUICK_BITES_DATA: Record<string, QuickBiteData> = {\n  \"glo-dollar\": gloDollar,\n};\n\n"
        "export default QUICK_BITES_DATA;\n"
    )
    out = register_quick_bite(index_ts, "ethereum-fee-spike-2026-10-29")
    assert "import ethereumFeeSpike20261029 from './qb-ethereum-fee-spike-2026-10-29';\n" in out
    assert '  "glo-dollar": gloDollar,\n  "ethereum-fee-spike-2026-10-29": ethereumFeeSpike20261029,\n};' in out


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"ok  {name}")
