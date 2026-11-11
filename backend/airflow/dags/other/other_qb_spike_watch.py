from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

# Spikes investigated per run. Each investigation is one Claude agent run (capped at MAX_BUDGET_USD in investigate.py).
MAX_EVENTS_PER_RUN = 1
# Days to check: yesterday plus the day before, in case a metric landed late.
CHECK_DAYS = 2


@dag(
    default_args={
        'owner': 'mseidl',
        'retries': 0,  # a retry would re-run the agent and could open a second PR
        'email_on_failure': False,
        'on_failure_callback': alert_via_webhook,
    },
    dag_id='other_qb_spike_watch',
    description='Find metric spikes, explain them with a Claude agent and open draft quick bite PRs on gtp-frontend',
    tags=['other', 'quick-bites'],
    start_date=datetime(2026, 11, 6),
    schedule='40 6 * * *',  # after the daily metric DAGs have filled fact_kpis and after api_json_quick_bites (05:20)
    catchup=False,
)
def qb_spike_watch():

    @task()
    def detect() -> list:
        """Scan fact_kpis for spikes, record them, and return the groups to investigate."""
        from datetime import date
        import pandas as pd
        from src.db_connector import DbConnector
        from src.main_config import get_main_config
        from src.misc.jinja_helper import execute_jinja_query
        from src.quick_bites.spike_watch.detect import BASELINE_DAYS, SPIKE_METRICS, drop_known, find_spikes, group_by_event

        db = DbConnector()
        chains = [c.origin_key for c in get_main_config() if c.api_in_main and c.api_deployment_flag == 'PROD']
        df = execute_jinja_query(db, 'api/quick_bites/spike_watch_candidates.sql.j2', {
            'origin_keys': chains, 'metric_keys': list(SPIKE_METRICS), 'days': BASELINE_DAYS + CHECK_DAYS + 2,
        }, return_df=True)

        today = date.today()
        check_dates = [(today - timedelta(days=d)).isoformat() for d in range(CHECK_DAYS, 0, -1)]
        spikes = find_spikes(df, check_dates)
        new, suppressed = drop_known(spikes, db.get_quick_bite_spike_events(days=30))
        print(f"{len(spikes)} spikes on {check_dates}: {len(new)} new, {len(suppressed)} follow-up days suppressed")

        groups = group_by_event(new)
        to_investigate = groups[:MAX_EVENTS_PER_RUN]
        now = pd.Timestamp.utcnow()
        rows = []
        for i, group in enumerate(groups):
            for sp in group:
                rows.append({**sp.to_dict(), 'status': 'detected' if i < MAX_EVENTS_PER_RUN else 'over_daily_cap', 'detected_at': now, 'updated_at': now})
        for sp in suppressed:
            rows.append({**sp.to_dict(), 'status': 'suppressed', 'detected_at': now, 'updated_at': now})
        if rows:
            out = pd.DataFrame(rows).drop(columns=['score']).set_index(['origin_key', 'metric_key', 'date'])
            db.upsert_table('quick_bite_spike_events', out)

        return [[sp.to_dict() for sp in group] for group in to_investigate]

    @task()
    def investigate_and_draft(groups: list):
        """Run the agent for each group; upload chart data and open a draft PR when it finds a story."""
        import os
        import pandas as pd
        from src.db_connector import DbConnector
        from src.main_config import get_main_config
        from src.quick_bites.spike_watch.detect import SPIKE_METRICS
        from src.quick_bites.spike_watch.investigate import investigate
        from src.quick_bites.spike_watch.publish import existing_slugs, frontend_repo, open_draft_pr, upload_charts
        from src.quick_bites.spike_watch.render import slugify, unique_slug

        if not groups:
            print("No spikes to investigate.")
            return
        db = DbConnector()
        chains = {c.origin_key: c for c in get_main_config()}
        labels = {k: v['label'] for k, v in SPIKE_METRICS.items()}
        dry_run = os.getenv('QB_SPIKE_WATCH_DRY_RUN', 'false').lower() == 'true'

        def update(group, **fields):
            out = pd.DataFrame([{'origin_key': sp['origin_key'], 'metric_key': sp['metric_key'], 'date': sp['date'],
                                 **fields, 'updated_at': pd.Timestamp.utcnow()} for sp in group])
            db.upsert_table('quick_bite_spike_events', out.set_index(['origin_key', 'metric_key', 'date']))

        for group in groups:
            origin_key = group[0]['origin_key']
            chain = chains[origin_key]
            update(group, status='investigating')

            result = investigate(db, chain.name, origin_key, group, labels)
            cost = round(result.cost_usd, 2)
            print(f"Agent finished for {origin_key} {group[0]['date']}: {result.num_turns} turns, ${cost}")
            if result.spec is None:
                update(group, status='failed', error=(result.error or '')[:1000], agent_cost_usd=cost)
                raise RuntimeError(f"Spike investigation failed for {origin_key}: {result.error}")

            spec = result.spec
            if not spec['newsworthy']:
                update(group, status='not_newsworthy', headline=spec['skip_reason'][:1000], agent_cost_usd=cost)
                continue
            if dry_run:
                print(spec)
                update(group, status='dry_run', headline=spec['headline'][:1000], agent_cost_usd=cost)
                continue

            repo = frontend_repo()
            slug = unique_slug(slugify(spec['slug']), existing_slugs(repo))
            chart_paths = upload_charts(slug, result.charts)
            pr_url = open_draft_pr(repo, spec, slug, result.charts, group, chain.name, origin_key,
                                   chain.colors['light'][0], cost, chart_paths)
            update(group, status='pr_opened', slug=slug, pr_url=pr_url, headline=spec['headline'][:1000], agent_cost_usd=cost)
            print(f"Opened draft PR: {pr_url}")

    investigate_and_draft(detect())


qb_spike_watch()
