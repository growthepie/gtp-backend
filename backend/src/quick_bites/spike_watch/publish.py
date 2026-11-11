"""Upload a spike quick bite's chart data and open a draft PR on gtp-frontend."""
import os

from src.quick_bites.spike_watch.render import (
    chart_jsons, chart_path, quick_bite_ts, register_quick_bite, S3_PREFIX,
)

FRONTEND_REPO = "growthepie/gtp-frontend"
FRONTEND_BASE = "main"
INDEX_PATH = "lib/quick-bites/index.ts"


def frontend_repo():
    from github import Auth, Github
    return Github(auth=Auth.Token(os.environ["GITHUB_GROWTHEPAI_TOKEN"])).get_repo(FRONTEND_REPO)


def existing_slugs(repo) -> set[str]:
    import re
    index_ts = repo.get_contents(INDEX_PATH, ref=FRONTEND_BASE).decoded_content.decode()
    return set(re.findall(r'^\s*"([a-z0-9-]+)":', index_ts, flags=re.MULTILINE))


def upload_charts(slug: str, charts: dict) -> list[str]:
    from src.misc.helper_functions import empty_cloudfront_cache, fix_dict_nan, upload_json_to_cf_s3
    bucket, cf_id = os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID")
    paths = []
    for name, body in chart_jsons(charts).items():
        body = fix_dict_nan(body, f"spike_{slug}_{name}", send_notification=False)
        upload_json_to_cf_s3(bucket, chart_path(slug, name), body, cf_id, invalidate=False)
        paths.append(chart_path(slug, name))
    empty_cloudfront_cache(cf_id, f"/{S3_PREFIX}/{slug}/*")
    return paths


def _pr_body(spec: dict, spikes: list[dict], warnings: list[str], cost_usd: float, chart_paths: list[str]) -> str:
    rows = "\n".join(
        f"| {s['metric_key']} | {s['date']} | {s['value']:,.4g} | {s['baseline_median']:,.4g} | {s['ratio']:.1f}x |" for s in spikes
    )
    sources = "\n".join(f"- [{s['title']}]({s['url']}): {s['used_for']}" for s in spec["sources"]) or "- none"
    unverified = "\n".join(f"- {c}" for c in spec["unverified_claims"]) or "- none"
    warn = "\n".join(f"- {w}" for w in warnings) or "- none"
    charts = "\n".join(f"- https://api.growthepie.com/{p}.json" for p in chart_paths)
    return f"""Drafted automatically by the backend spike watch (`other_qb_spike_watch`). **Review before merging.**

**Finding:** {spec['headline']}
**Agent confidence:** {spec['confidence']}

### Spike that triggered this
| Metric | Date | Value | 28-day median | Ratio |
|---|---|---|---|---|
{rows}

### Sources the agent used
{sources}

### Claims the agent could not verify
{unverified}

### Rendering warnings
{warn}

### Chart data (already uploaded)
{charts}

### Before merging
- [ ] Check every number against growthepie.com
- [ ] Check the off-chain cause against the sources
- [ ] Add a banner image at `public/quick-bites/<slug>.webp`
- [ ] Check the charts in the preview deployment

Method: {spec['method_note']}
Agent cost: ${cost_usd:.2f}
"""


def open_draft_pr(repo, spec: dict, slug: str, charts: dict, spikes: list[dict], chain_name: str, origin_key: str,
                  color: str, cost_usd: float, chart_paths: list[str]) -> str:
    ts, warnings = quick_bite_ts(spec, slug, charts, chain_name, origin_key, color)
    branch = f"qb-spike/{slug}"
    base_sha = repo.get_branch(FRONTEND_BASE).commit.sha
    repo.create_git_ref(ref=f"refs/heads/{branch}", sha=base_sha)
    repo.create_file(f"lib/quick-bites/qb-{slug}.ts", f"feat(quick-bites): draft {slug}", ts, branch=branch)
    index = repo.get_contents(INDEX_PATH, ref=branch)
    repo.update_file(INDEX_PATH, f"feat(quick-bites): register {slug}",
                     register_quick_bite(index.decoded_content.decode(), slug), index.sha, branch=branch)
    pr = repo.create_pull(title=f"Quick bite draft: {spec['title']}", body=_pr_body(spec, spikes, warnings, cost_usd, chart_paths),
                          head=branch, base=FRONTEND_BASE, draft=True)
    return pr.html_url
