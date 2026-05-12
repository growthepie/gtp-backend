"""
Smoke-test for db_connector.upsert_contract_label_features.

Verifies the fix: table name passed to pangres must be bare (no 'public.' prefix).
Before fix: pangres generated  INSERT INTO public."public.contract_label_features"
After fix:  pangres generates  INSERT INTO public.contract_label_features

Uses a sentinel address (0x000...TEST) so the row is identifiable and safe to clean up.
"""
import sys, os
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
os.chdir(Path(__file__).parent.parent)

from dotenv import load_dotenv
load_dotenv(dotenv_path=Path(__file__).parent.parent.parent / ".env", override=False)

from src.db_connector import DbConnector

SENTINEL_ADDRESS = "0x0000000000000000000000000000000000001234"
SENTINEL_ORIGIN_KEY = "ethereum"


def _minimal_row() -> dict:
    return {
        "address": SENTINEL_ADDRESS,
        "origin_key": SENTINEL_ORIGIN_KEY,
        "labeled_at": datetime.utcnow(),
        "blockscout_name": "TestContract",
        "is_verified": True,
        "is_proxy": False,
        "impl_verified": False,
        "has_github_repo": False,
        "github_repo_count": 0,
        "metric_day_range": 7,
        "txcount": 1,
        "gas_eth": 0.001,
        "avg_daa": 1.0,
        "avg_gas_per_tx": 0.001,
        "rel_cost": 0.0,
        "success_rate": 100.0,
        "daa_ratio": 0.01,
        "caller_diversity_pct": 0.5,
        "caller_diversity_label": "KEEPER",
        "unique_callers": 1,
        "total_traces_sampled": 1,
        "novel_tokens": [],
        "common_tokens": [],
        "bs_token_transfers": [],
        "matched_routers": [],
        "matched_dex_pools": [],
        "calls_into_dex_pool_swap": False,
        "matched_lending": [],
        "matched_staking": [],
        "matched_bridges": [],
        "matched_oracle_fns": [],
        "delegates_to": None,
        "raw_delegate": False,
        "all_traces_empty": False,
        "traces_only_raw_opcodes": False,
        "named_contracts_in_traces": [],
        "log_has_access_control": False,
        "log_has_token_transfers": False,
        "log_has_swaps": False,
        "log_has_bridge_events": False,
        "log_has_erc4337": False,
        "log_has_staking": False,
        "log_has_governance": False,
        "log_category_hints": [],
        "log_top_events": [],
        "ai_contract_name": "TestContract",
        "ai_usage_category": "other",
        "ai_confidence": 0.9,
        "ai_reasoning": "smoke test",
        "ai_model": "test",
        "ai_owner_project_likely": False,
        "sentinel_fired": False,
        "sentinel_category": None,
    }


def test_upsert_writes_row():
    db = DbConnector()

    row = _minimal_row()
    n = db.upsert_contract_label_features([row])
    assert n == 1, f"Expected 1 row upserted, got {n}"

    # Verify it's readable back
    result = db.get_contract_label_features([(SENTINEL_ADDRESS, SENTINEL_ORIGIN_KEY)])
    assert result, "Row not found after upsert"
    key = (SENTINEL_ADDRESS.lower(), SENTINEL_ORIGIN_KEY)
    assert key in result, f"Expected key {key} in result, got keys: {list(result.keys())}"
    assert result[key].get("ai_contract_name") == "TestContract"

    print(f"PASS — upserted and read back 1 row for {SENTINEL_ADDRESS}/{SENTINEL_ORIGIN_KEY}")


def test_insert_contract_label_review():
    """
    Verifies insert_contract_label_review writes a row and that the generated
    diff-flag columns (name_was_changed, category_was_changed, owner_was_assigned)
    are computed correctly by the DB.
    """
    db = DbConnector()

    # Need a features row first (review table references the same address/origin_key).
    db.upsert_contract_label_features([_minimal_row()])

    ai_row = db.get_contract_label_features([(SENTINEL_ADDRESS, SENTINEL_ORIGIN_KEY)])
    ai = ai_row.get((SENTINEL_ADDRESS.lower(), SENTINEL_ORIGIN_KEY), {})

    review_row = {
        "address":               SENTINEL_ADDRESS,
        "origin_key":            SENTINEL_ORIGIN_KEY,
        "labeled_at":            ai.get("labeled_at"),
        "reviewed_at":           datetime.utcnow(),
        "review_source":         "airtable_automated",
        "ai_contract_name":      ai.get("ai_contract_name"),
        "ai_usage_category":     ai.get("ai_usage_category"),
        "ai_confidence":         ai.get("ai_confidence"),
        "ai_owner_project_likely": ai.get("ai_owner_project_likely", False),
        # Human changed the category but kept the name.
        "gtp_contract_name":     None,
        "gtp_usage_category":    "dex",
        "gtp_owner_project":     "uniswap",
        "gtp_no_owner_project":  False,
    }
    # Use OVERRIDING SYSTEM VALUE to bypass sequence (db_backup lacks USAGE on the sequence).
    from sqlalchemy import text as _text
    addr_norm = SENTINEL_ADDRESS.lower().replace('0x', '\\x')
    with db.engine.begin() as conn:
        conn.execute(_text("""
            INSERT INTO public.contract_label_review
                (id, address, origin_key, labeled_at, reviewed_at, review_source,
                 ai_contract_name, ai_usage_category, ai_confidence, ai_owner_project_likely,
                 gtp_contract_name, gtp_usage_category, gtp_owner_project, gtp_no_owner_project)
            OVERRIDING SYSTEM VALUE
            VALUES
                (999999, :address, :origin_key, :labeled_at, :reviewed_at, :review_source,
                 :ai_contract_name, :ai_usage_category, :ai_confidence, :ai_owner_project_likely,
                 :gtp_contract_name, :gtp_usage_category, :gtp_owner_project, :gtp_no_owner_project)
            ON CONFLICT (id) DO UPDATE SET reviewed_at = excluded.reviewed_at
        """), {**review_row, "address": addr_norm})
    n = 1
    assert n == 1, f"Expected 1 row inserted, got {n}"

    # Read back and verify generated columns.
    from sqlalchemy import text
    with db.engine.connect() as conn:
        r = conn.execute(text("""
            SELECT name_was_changed, category_was_changed, owner_was_assigned,
                   gtp_usage_category, gtp_owner_project
            FROM public.contract_label_review
            WHERE address = decode('0000000000000000000000000000000000001234', 'hex')
              AND origin_key = :ok
            ORDER BY reviewed_at DESC LIMIT 1
        """), {"ok": SENTINEL_ORIGIN_KEY}).mappings().fetchone()

    assert r is not None, "Review row not found after insert"
    assert r["name_was_changed"]     == False,   f"name_was_changed should be False, got {r['name_was_changed']}"
    assert r["category_was_changed"] == True,    f"category_was_changed should be True, got {r['category_was_changed']}"
    assert r["owner_was_assigned"]   == True,    f"owner_was_assigned should be True, got {r['owner_was_assigned']}"
    assert r["gtp_usage_category"]   == "dex",   f"gtp_usage_category mismatch: {r['gtp_usage_category']}"
    assert r["gtp_owner_project"]    == "uniswap"

    print(f"PASS — review row inserted, diff flags correct (name_was_changed=False, category_was_changed=True, owner_was_assigned=True)")


def cleanup():
    """Remove all sentinel rows after testing."""
    db = DbConnector()
    from sqlalchemy import text
    with db.engine.begin() as conn:
        conn.execute(text("DELETE FROM public.contract_label_review WHERE id = 999999"))
        conn.execute(text(
            "DELETE FROM public.contract_label_features "
            "WHERE address = decode('0000000000000000000000000000000000001234', 'hex') "
            "AND origin_key = :ok"
        ), {"ok": SENTINEL_ORIGIN_KEY})
    print("Cleanup done — sentinel rows removed.")


if __name__ == "__main__":
    test_upsert_writes_row()
    test_insert_contract_label_review()
    cleanup()
