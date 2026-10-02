"""Read-only audit of the first Verifier Alliance live pilot.

Run from backend/: python misc/audit_verifier_alliance_pilot.py
Uses attestation timestamps, not a historical snapshot of database insertion time.
"""
import argparse
import json
import os
from datetime import date, datetime, time, timedelta, timezone
from urllib.parse import urlsplit

import psycopg2
from dotenv import load_dotenv
from web3 import Web3


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default="2026-10-02", type=date.fromisoformat)
    parser.add_argument("--limit", default=1000, type=int)
    parser.add_argument("--attester", default="0xa725646c05e6bb813d98c5abb4e72df4bcf00b56")
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be positive")
    load_dotenv()
    start = datetime.combine(args.date, time.min, timezone.utc)
    endpoint = urlsplit("//" + os.environ["DB_HOST"])
    con = psycopg2.connect(host=endpoint.hostname,
                           port=endpoint.port or int(os.getenv("DB_PORT", "5432")),
                           user=os.environ["DB_USERNAME"],
                           password=os.environ["DB_PASSWORD"], dbname="oli", connect_timeout=15)
    try:
        con.set_session(readonly=True, isolation_level="REPEATABLE READ")
        with con.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout='180s'")
            cur.execute("""
                SELECT time, recipient, chain_id, tags_json
                FROM public.attestations
                WHERE attester = %s AND time >= %s AND time < %s
                  AND tags_json->>'_source' = 'https://verifieralliance.org/'
                ORDER BY time, uid LIMIT %s
            """, (bytes.fromhex(args.attester.removeprefix("0x")), start, start + timedelta(days=1), args.limit))
            rows = cur.fetchall()
            if len(rows) != args.limit:
                raise RuntimeError(f"Expected {args.limit} pilot attestations, found {len(rows)}; check date/attester")
            cutoff = min(row[0] for row in rows)
            pairs = {(chain, address.lower()) for _, address, chain, _ in rows}
            addresses = {address for _, address in pairs}
            # OLI normally stores lowercase recipients. Include checksummed form
            # for older records, without casting the indexed recipient column.
            variants = sorted(addresses | {Web3.to_checksum_address(a) for a in addresses})
            cur.execute("""
                SELECT DISTINCT chain_id, recipient, revoked
                FROM public.attestations
                WHERE recipient = ANY(CAST(%s AS text[])) AND time < %s
            """, (variants, cutoff))
            previous = cur.fetchall()
            old_pairs = {(chain, address.lower()) for chain, address, _ in previous}
            active_pairs = {(chain, address.lower()) for chain, address, revoked in previous if revoked is False}
            old_addresses = {address.lower() for _, address, _ in previous}
            tag_counts = {}
            for _, _, _, tags in rows:
                if isinstance(tags, str):
                    tags = json.loads(tags)
                for tag in tags:
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1
            print(json.dumps({
                "pilot_attestations": len(rows),
                "pilot_first_attestation": cutoff.isoformat(),
                "pilot_last_attestation": max(row[0] for row in rows).isoformat(),
                "distinct_chain_address_pairs": len(pairs),
                "pairs_with_earlier_attestations": len(pairs & old_pairs),
                "pairs_without_earlier_attestations": len(pairs - old_pairs),
                "pairs_with_earlier_currently_nonrevoked_attestations": len(pairs & active_pairs),
                "distinct_addresses_ignoring_chain": len(addresses),
                "addresses_without_earlier_attestations_on_any_chain": len(addresses - old_addresses),
                "submitted_tag_counts": tag_counts,
                "scope": "First N importer attestations on the selected UTC day; verify timestamps match your pilot.",
                "caveat": "Earlier means attestation time before pilot start, not proven DB insertion time. Includes revoked historical records in existence counts; deleted history is unavailable.",
            }, indent=2))
    finally:
        con.close()


if __name__ == "__main__":
    main()
