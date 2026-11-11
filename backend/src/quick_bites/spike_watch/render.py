"""Turn the agent's draft into chart JSONs for api.growthepie.com and a quick bite file for gtp-frontend."""
import json
import re
from datetime import date

API_BASE = "https://api.growthepie.com"
S3_PREFIX = "v1/quick-bites/spikes"
AUTHOR = {"name": "growthepie research", "xUsername": "growthepie_eth"}
SERIES_COLORS = ["#FE5468", "#94ABD3", "#FFDF27", "#19D9D6", "#FB923C"]


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug[:60].strip("-") or "spike"


def unique_slug(slug: str, taken: set[str]) -> str:
    candidate, n = slug, 2
    while candidate in taken:
        candidate, n = f"{slug}-{n}", n + 1
    return candidate


def var_name(slug: str) -> str:
    parts = slug.split("-")
    name = parts[0] + "".join(p.capitalize() for p in parts[1:])
    return name if name[0].isalpha() else f"qb{name[0].upper()}{name[1:]}"


def chart_path(slug: str, data_name: str) -> str:
    return f"{S3_PREFIX}/{slug}/{data_name}"


def chart_jsons(charts: dict) -> dict:
    """data_name -> JSON body in the shape the quick bite charts read (data.series.values)."""
    return {name: {"data": {"series": {"types": c["types"], "values": c["values"]}}, "description": c["description"]}
            for name, c in charts.items()}


def _chart_block(chart: dict, slug: str, charts: dict, color: str, warnings: list) -> list[str]:
    meta = []
    for i, s in enumerate(chart["series"]):
        stored = charts.get(s["data_name"])
        if stored is None or s["y_index"] >= len(stored["types"]):
            warnings.append(f"Dropped series '{s['label']}' in chart '{chart['title']}': no stored data '{s['data_name']}' with column {s['y_index']}.")
            continue
        entry = {
            "name": s["label"],
            "color": color if i == 0 else SERIES_COLORS[i % len(SERIES_COLORS)],
            "type": s.get("series_type", chart["chart_type"]),
            "xIndex": 0,
            "yIndex": s["y_index"],
            "url": f"{API_BASE}/{chart_path(slug, s['data_name'])}.json",
            "pathToData": "data.series.values",
        }
        for src, dst in (("prefix", "prefix"), ("suffix", "suffix"), ("decimals", "tooltipDecimals")):
            if s.get(src) not in (None, ""):
                entry[dst] = s[src]
        if s.get("opposite_axis"):
            entry["oppositeYAxis"] = True
        meta.append(entry)
    if not meta:
        return []
    body = {"type": chart["chart_type"], "title": chart["title"], "subtitle": chart.get("subtitle", ""),
            "showXAsDate": True, "dataAsJson": {"meta": meta}, "height": 400}
    if chart.get("caption"):
        body["caption"] = chart["caption"]
    return ["```chart", body, "```"]


def content_blocks(spec: dict, slug: str, charts: dict, color: str, warnings: list) -> list:
    """Content as a list of strings and dicts (dicts become JSON.stringify(...) in the TS file)."""
    out: list = []
    for b in spec["blocks"]:
        kind = b["kind"]
        if kind == "heading" and b.get("text"):
            out.append(f"# {b['text']}")
        elif kind == "paragraph" and b.get("text"):
            out.append(b["text"])
        elif kind == "callout" and b.get("text"):
            out.append(f"> {b['text']}")
        elif kind == "bullets" and b.get("items"):
            out += [f"- {item}" for item in b["items"]]
        elif kind == "chart" and b.get("chart"):
            out += _chart_block(b["chart"], slug, charts, color, warnings)
        elif kind == "kpis" and b.get("kpis"):
            cards = [{"title": k["title"], "value": k["value"], "icon": "gtp-realtime", **({"info": k["info"]} if k.get("info") else {})}
                     for k in b["kpis"]]
            out += ["```kpi-cards", cards, "```"]
    if spec.get("method_note"):
        out.append(f"> How we checked this: {spec['method_note']}")
    if spec.get("sources"):
        out.append("# Sources")
        out += [f"- [{s['title']}]({s['url']})" for s in spec["sources"]]
    return out


def _ts_value(item) -> str:
    return f"JSON.stringify({json.dumps(item, ensure_ascii=False)})" if isinstance(item, (dict, list)) else json.dumps(item, ensure_ascii=False)


def quick_bite_ts(spec: dict, slug: str, charts: dict, chain_name: str, origin_key: str, color: str,
                  today: date | None = None) -> tuple[str, list[str]]:
    warnings: list[str] = []
    content = content_blocks(spec, slug, charts, color, warnings)
    short_title = spec["short_title"][:20].rstrip()
    obj = [
        ("title", json.dumps(spec["title"], ensure_ascii=False)),
        ("subtitle", json.dumps(spec["subtitle"], ensure_ascii=False)),
        ("shortTitle", json.dumps(short_title, ensure_ascii=False)),
        ("summary", json.dumps(spec["summary"], ensure_ascii=False)),
        ("kind", '"article"'),
        ("content", "[\n" + ",\n".join(f"    {_ts_value(c)}" for c in content) + ",\n  ]"),
        ("image", json.dumps(f"/quick-bites/{slug}.webp")),
        ("og_image", json.dumps(f"/quick-bites/{slug}.webp")),
        ("date", json.dumps((today or date.today()).isoformat())),
        ("related", "[]"),
        ("author", json.dumps([AUTHOR])),
        ("topics", json.dumps([{"name": chain_name, "url": f"/chains/{origin_key.replace('_', '-')}"}], ensure_ascii=False)),
        ("icon", json.dumps(f"{origin_key.replace('_', '-')}-logo-monochrome")),
        ("showInMenu", "true"),
    ]
    name = var_name(slug)
    body = ",\n".join(f"  {k}: {v}" for k, v in obj)
    ts = (
        "import { QuickBiteData } from '@/lib/types/quickBites';\n\n"
        f"// Drafted by the growthepie spike watch (backend DAG other_qb_spike_watch). Review before merging.\n"
        f"const {name}: QuickBiteData = {{\n{body},\n}};\n\nexport default {name};\n"
    )
    return ts, warnings


def register_quick_bite(index_ts: str, slug: str) -> str:
    """Add an import and a QUICK_BITES_DATA entry to lib/quick-bites/index.ts."""
    name = var_name(slug)
    import_line = f"import {name} from './qb-{slug}';\n"
    imports = list(re.finditer(r"^import .*;\n", index_ts, flags=re.MULTILINE))
    if not imports:
        raise ValueError("No imports found in index.ts")
    at = imports[-1].end()
    index_ts = index_ts[:at] + import_line + index_ts[at:]
    block = re.search(r"const QUICK_BITES_DATA[^=]*=\s*\{(.*?)\n\};", index_ts, flags=re.DOTALL)
    if not block:
        raise ValueError("QUICK_BITES_DATA not found in index.ts")
    insert_at = block.end() - len("\n};")
    return index_ts[:insert_at] + f'\n  "{slug}": {name},' + index_ts[insert_at:]
