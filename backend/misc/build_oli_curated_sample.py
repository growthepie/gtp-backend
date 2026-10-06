"""Build the approved frozen trial from downloaded growthepie top-contract JSON.

No network/DB access. Run manually after reviewing the inputs; never schedule
rotation, which would gradually expose more than the approved 100 contracts.
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

CHAINS = {"ethereum": "eip155:1", "base": "eip155:8453", "robinhood": "eip155:4663",
          "celo": "eip155:42220", "polygon_pos": "eip155:137"}


def build_sample(source_dir, snapshot_at):
    samples = []
    for chain, chain_id in CHAINS.items():
        rows = json.loads((source_dir / f"{chain}.json").read_text())
        rows.sort(key=lambda row: row.get("txcount_180d") or 0, reverse=True)
        seen = set()
        for row in rows:
            address = row["address"].lower()
            if address in seen:
                continue
            if row["chain_id"] != chain_id:
                raise ValueError(f"Unexpected chain ID in {chain} source")
            seen.add(address)
            tags = {"contract_name": row["name"]} if row.get("name") else {}
            for key in ["owner_project", "usage_category", "deployment_tx", "deployer_address", "deployment_date"]:
                if row.get(key) is not None:
                    tags[key] = row[key]
            samples.append({"address": address, "chain_id": chain_id, "tags": tags,
                            "snapshot_at": snapshot_at,
                            "source_url": f"https://api.growthepie.com/v1/top_contracts/export_{chain}.json",
                            "txcount_180d": int(row.get("txcount_180d") or 0)})
            if len(seen) == 20:
                break
    return samples


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_dir", type=Path)
    parser.add_argument("--output", type=Path, default=Path(__file__).parents[1] / "src/oli/api/curated_sample.json")
    args = parser.parse_args()
    sample = build_sample(args.source_dir, datetime.now(timezone.utc).isoformat())
    args.output.write_text(json.dumps(sample, indent=2, allow_nan=False) + "\n")
    print(f"Wrote {len(sample)} frozen samples to {args.output}")
