"""System prompt and output schema for the spike investigation agent."""

SYSTEM_PROMPT = """You are a data analyst at growthepie, an analytics site for Ethereum and its Layer 2s. \
An automated check found a spike in a chain metric. Find out what caused it, and if the cause is clear \
and interesting, draft a short "Quick Bite" article for growthepie.com that explains it to a general crypto audience.

## How to investigate
Work from growthepie's own data first, then go deeper only as far as you need:
1. growthepie MCP (mcp__growthepie__*): confirm the spike with get_chain_metric (daily, then hourly around the spike), \
compare related metrics (txcount, daa, txcosts, fees), see which apps drove it with top_apps and get_app_timeseries \
(daily and hourly, metric gas_fees or txcount), and check ETH burn, price and supply with get_eth_timeseries when relevant.
2. sql_read: growthepie's database, for what the MCP doesn't expose, such as fees per contract on one day. \
Keep queries narrow (one chain, a few days) and read-only.
3. rpc_call and block_at_time: blocks, receipts and logs, to see what was inside the busiest blocks (base fee, \
gas used, which contracts consumed the gas, which events they emitted, DEX swap prices). Sample blocks; don't scan whole hours.
4. WebSearch and WebFetch: news from the days around the spike, to explain off-chain causes. Only state an off-chain \
cause if at least two independent, reputable sources report it, and if its timing matches the on-chain data.
Third-party analytics are a last resort.

Check your own claims. If a number came from a sample, say so. If two sources disagree, say which you used. \
Prefer growthepie numbers on the page wherever they exist.

## When not to publish
Set newsworthy to false if the spike is a data artifact (backfill, a broken adapter, a relabeling), if you can't find a \
cause you'd defend, or if the story is trivial. Explain why in skip_reason. A wrong article is worse than none.

## Charts
Every chart reads data you stored with save_chart_data during this session. Store compact series: daily values over \
a few weeks around the spike, hourly values over the spike window, or block samples. The first column is "unix" in \
milliseconds. Use 2 to 4 charts. Prefer growthepie data for charts.

## Writing
Plain, short sentences for readers who are not analysts. Lead with the finding. Explain mechanisms (like EIP-1559 base \
fee increases) in one or two sentences. Use active voice, no hype, no emoji, no em dashes. Cite every outside claim in \
sources. Put anything you could not verify in unverified_claims. Times are UTC.
"""

QUICK_BITE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["newsworthy", "skip_reason", "headline", "confidence", "slug", "title", "subtitle", "short_title",
                 "summary", "blocks", "sources", "unverified_claims", "method_note"],
    "properties": {
        "newsworthy": {"type": "boolean"},
        "skip_reason": {"type": "string", "description": "Why not to publish; empty when newsworthy."},
        "headline": {"type": "string", "description": "One sentence: what happened and why."},
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        "slug": {"type": "string", "description": "kebab-case, e.g. ethereum-fee-spike-2026-10-29"},
        "title": {"type": "string"},
        "subtitle": {"type": "string"},
        "short_title": {"type": "string", "maxLength": 20},
        "summary": {"type": "string", "description": "Two sentences for SEO."},
        "blocks": {
            "type": "array",
            "description": "The article body in order.",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["kind"],
                "properties": {
                    "kind": {"type": "string", "enum": ["heading", "paragraph", "bullets", "callout", "chart", "kpis"]},
                    "text": {"type": "string", "description": "heading, paragraph or callout text (markdown allowed)"},
                    "items": {"type": "array", "items": {"type": "string"}, "description": "bullets"},
                    "chart": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["title", "chart_type", "series"],
                        "properties": {
                            "title": {"type": "string"},
                            "subtitle": {"type": "string"},
                            "caption": {"type": "string"},
                            "chart_type": {"type": "string", "enum": ["line", "column", "area"]},
                            "series": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "required": ["data_name", "y_index", "label"],
                                    "properties": {
                                        "data_name": {"type": "string", "description": "name given to save_chart_data"},
                                        "y_index": {"type": "integer", "minimum": 1},
                                        "label": {"type": "string"},
                                        "series_type": {"type": "string", "enum": ["line", "column", "area"]},
                                        "prefix": {"type": "string"},
                                        "suffix": {"type": "string"},
                                        "decimals": {"type": "integer", "minimum": 0, "maximum": 6},
                                        "opposite_axis": {"type": "boolean"},
                                    },
                                },
                            },
                        },
                    },
                    "kpis": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["title", "value"],
                            "properties": {
                                "title": {"type": "string"},
                                "value": {"type": "string"},
                                "info": {"type": "string"},
                            },
                        },
                    },
                },
            },
        },
        "sources": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["title", "url", "used_for"],
                "properties": {"title": {"type": "string"}, "url": {"type": "string"}, "used_for": {"type": "string"}},
            },
        },
        "unverified_claims": {"type": "array", "items": {"type": "string"}},
        "method_note": {"type": "string", "description": "Which data sources and samples the numbers come from."},
    },
}


def task_prompt(chain_name: str, origin_key: str, spikes: list[dict], metric_labels: dict) -> str:
    lines = [f"Chain: {chain_name} (origin_key: {origin_key})", "Spikes found on this chain:"]
    for s in spikes:
        label = metric_labels.get(s["metric_key"], s["metric_key"])
        lines.append(
            f"- {label} ({s['metric_key']}) on {s['date']}: {s['value']:,.4g}, "
            f"{s['ratio']:.1f}x the {s['baseline_median']:,.4g} median of the previous 28 days (robust z {s['robust_z']:.0f})"
        )
    lines.append(
        "\nInvestigate what caused this, store the chart data you need with save_chart_data, "
        "and return the quick bite draft (or newsworthy=false with a reason)."
    )
    return "\n".join(lines)
