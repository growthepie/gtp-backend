"""
Backfill contract_label_review from verified GTP attester labels.

Joins contract_label_features (AI baseline) against public.labels
(GTP attester pivot) and inserts diff rows into contract_label_review.

Safe to re-run — skips (address, origin_key) pairs already present.

Usage:
    python backend/misc/backfill_label_review.py [--dry-run]
"""
import sys, os, argparse
from pathlib import Path

_BACKEND = Path(__file__).parent.parent
sys.path.insert(0, str(_BACKEND))
os.chdir(_BACKEND)

from dotenv import load_dotenv
load_dotenv(dotenv_path=_BACKEND / ".env", override=False)
load_dotenv(dotenv_path=_BACKEND.parent / ".env", override=False)

import pandas as pd
from sqlalchemy import text
from src.db_connector import DbConnector

GTP_ATTESTER = 'A725646C05E6BB813D98C5ABB4E72DF4BCF00B56'  # no 0x prefix for decode()

PIVOT_SQL = text("""
    WITH latest_labels AS (
        -- labels.address is text ('0x'-prefixed lowercase hex), not bytea
        SELECT
            l.address,
            l.chain_id,
            MAX(CASE WHEN l.tag_id = 'contract_name'  THEN l.tag_value END) AS gtp_name,
            MAX(CASE WHEN l.tag_id = 'usage_category' THEN l.tag_value END) AS gtp_cat,
            MAX(CASE WHEN l.tag_id = 'owner_project'  THEN l.tag_value END) AS gtp_owner,
            MAX(l.time) AS attested_at
        FROM public.labels l
        WHERE l.attester = decode(:attester, 'hex')
        GROUP BY l.address, l.chain_id
    ),
    already_reviewed AS (
        SELECT address, origin_key FROM public.contract_label_review
    )
    SELECT
        '0x' || encode(f.address, 'hex')   AS address,
        f.origin_key,
        f.labeled_at,
        ll.attested_at                      AS reviewed_at,
        f.ai_contract_name,
        f.ai_usage_category,
        f.ai_confidence,
        f.ai_owner_project_likely,
        ll.gtp_name,
        ll.gtp_cat,
        ll.gtp_owner
    FROM public.contract_label_features f
    JOIN public.sys_main_conf s   ON s.origin_key = f.origin_key
    JOIN latest_labels ll
        ON ll.address   = '0x' || encode(f.address, 'hex')
        AND ll.chain_id = s.caip2
    LEFT JOIN already_reviewed ar
        ON ar.address    = f.address
        AND ar.origin_key = f.origin_key
    WHERE ar.address IS NULL
""")


def build_review_rows(df: pd.DataFrame) -> list[dict]:
    """Transform pivot DataFrame into insert-ready dicts for insert_contract_label_review."""
    rows = []
    for _, r in df.iterrows():
        ai_name = r['ai_contract_name'] if pd.notna(r['ai_contract_name']) else None
        ai_cat  = r['ai_usage_category'] if pd.notna(r['ai_usage_category']) else None
        gtp_name  = r['gtp_name']  if pd.notna(r['gtp_name'])  else None
        gtp_cat   = r['gtp_cat']   if pd.notna(r['gtp_cat'])   else None
        gtp_owner = r['gtp_owner'] if pd.notna(r['gtp_owner']) else None

        rows.append({
            'address':               r['address'],
            'origin_key':            r['origin_key'],
            'labeled_at':            r['labeled_at'],
            'reviewed_at':           r['reviewed_at'],
            'review_source':         'labels_backfill',
            'ai_contract_name':      ai_name,
            'ai_usage_category':     ai_cat,
            'ai_confidence':         float(r['ai_confidence']) if pd.notna(r['ai_confidence']) else None,
            'ai_owner_project_likely': bool(r['ai_owner_project_likely']) if pd.notna(r['ai_owner_project_likely']) else None,
            'gtp_contract_name':  gtp_name  if gtp_name  != ai_name else None,
            'gtp_usage_category': gtp_cat   if gtp_cat   != ai_cat  else None,
            'gtp_owner_project':  gtp_owner,
            'gtp_no_owner_project': False,
        })
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true',
                        help='Print rows that would be inserted without writing')
    args = parser.parse_args()

    db = DbConnector()
    with db.engine.begin() as conn:
        df = pd.read_sql(PIVOT_SQL, conn, params={'attester': GTP_ATTESTER})

    print(f"Found {len(df)} contracts with GTP labels + features baseline")
    print(df[['address', 'origin_key', 'ai_usage_category', 'gtp_cat', 'gtp_name', 'ai_contract_name']].to_string())

    rows = build_review_rows(df)

    changed_name = sum(1 for r in rows if r['gtp_contract_name'] is not None)
    changed_cat  = sum(1 for r in rows if r['gtp_usage_category'] is not None)
    has_owner    = sum(1 for r in rows if r['gtp_owner_project'] is not None)
    print(f"\nDiff summary:")
    print(f"  name changed:     {changed_name}")
    print(f"  category changed: {changed_cat}")
    print(f"  owner assigned:   {has_owner}")

    if args.dry_run:
        print("\n[dry-run] No rows written.")
        return

    # insert_contract_label_review uses the sequence for id; db_backup lacks USAGE on
    # the sequence, so we supply explicit ids via raw psycopg2 to avoid that permission.
    import psycopg2, psycopg2.extras
    engine_url = db.engine.url
    raw = psycopg2.connect(
        host=engine_url.host,
        port=engine_url.port or 5432,
        dbname=engine_url.database,
        user=engine_url.username,
        password=engine_url.password,
    )
    cur = raw.cursor()
    cur.execute("SELECT COALESCE(MAX(id), 0) FROM public.contract_label_review")
    next_id = cur.fetchone()[0] + 1

    insert_sql = """
        INSERT INTO public.contract_label_review (
            id, address, origin_key, labeled_at, reviewed_at, review_source,
            ai_contract_name, ai_usage_category, ai_confidence, ai_owner_project_likely,
            gtp_contract_name, gtp_usage_category, gtp_owner_project, gtp_no_owner_project
        ) VALUES (
            %(id)s,
            decode(%(address_hex)s, 'hex'),
            %(origin_key)s, %(labeled_at)s, %(reviewed_at)s, %(review_source)s,
            %(ai_contract_name)s, %(ai_usage_category)s, %(ai_confidence)s, %(ai_owner_project_likely)s,
            %(gtp_contract_name)s, %(gtp_usage_category)s, %(gtp_owner_project)s, %(gtp_no_owner_project)s
        )
    """
    params = []
    for i, r in enumerate(rows):
        addr = r['address']
        # strip leading \x or 0x to get raw hex for decode()
        if addr.startswith('0x') or addr.startswith('\\x'):
            addr_hex = addr[2:]
        else:
            addr_hex = addr
        params.append({
            'id': next_id + i,
            'address_hex': addr_hex,
            'origin_key': r['origin_key'],
            'labeled_at': r['labeled_at'],
            'reviewed_at': r['reviewed_at'],
            'review_source': r['review_source'],
            'ai_contract_name': r['ai_contract_name'],
            'ai_usage_category': r['ai_usage_category'],
            'ai_confidence': r['ai_confidence'],
            'ai_owner_project_likely': r['ai_owner_project_likely'],
            'gtp_contract_name': r['gtp_contract_name'],
            'gtp_usage_category': r['gtp_usage_category'],
            'gtp_owner_project': r['gtp_owner_project'],
            'gtp_no_owner_project': r['gtp_no_owner_project'],
        })

    psycopg2.extras.execute_batch(cur, insert_sql, params)
    raw.commit()
    raw.close()
    n = len(params)
    print(f"\nInserted {n} rows into contract_label_review.")


if __name__ == '__main__':
    main()
