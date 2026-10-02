"""Offline tests: no production DB, credentials, or OLI submissions."""

import json
import tempfile
import unittest
import sqlite3
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq
from jsonschema import ValidationError

from src.adapters import adapter_verifier_alliance as va

DEFINITIONS = {"source_code_verified": {"schema": {"type": "string", "enum": ["sourcify", "blockscout", "etherscan"]}}}


def fixtures():
    verification = dict(id=1, deployment_id="d", compilation_id="c", created_at="2026-01-01",
                        created_by="sourcify", creation_match=True, runtime_match=False,
                        creation_metadata_match=False, runtime_metadata_match=None)
    deployment = dict(id="d", chain_id=1, address=b"\x12" * 20,
                      transaction_hash=b"\x34" * 32, block_number=0, deployer=b"\x56" * 20)
    compilation = dict(id="c", name="Token", compiler="solc", version="0.8.20", language="Solidity")
    return verification, deployment, compilation


class VerifierAllianceTests(unittest.TestCase):
    def setUp(self):
        allowed = patch.object(va, "allowed_verifiers", return_value={"sourcify", "blockscout", "etherscan"})
        allowed.start()
        self.addCleanup(allowed.stop)
        roles = patch.object(va, "verifier_roles", return_value={"sourcify"})
        roles.start()
        self.addCleanup(roles.stop)
        self.state = va.State(":memory:")
        self.verification, self.deployment, self.compilation = fixtures()
        self.label = va.make_label(self.verification, self.deployment, self.compilation, {"sourcify": "sourcify"})

    def tearDown(self):
        self.state.db.close()

    def test_mapping_retains_verification_evidence_without_inventing_fields(self):
        tags = self.label["tags"]
        self.assertEqual(tags["source_code_verified"], "sourcify")
        self.assertEqual(tags["deployment_block"], 0)
        self.assertEqual(tags["code_language"], "solidity")
        self.assertNotIn("deployment_date", tags)
        self.assertFalse(json.loads(tags["_comment"])["verifier_alliance"]["runtime_match"])
        with self.assertRaises(va.UnknownVerifierError):
            va.make_label({**self.verification, "created_by": "routescan"}, self.deployment, self.compilation, {})

    def test_invalid_address_fails(self):
        with self.assertRaises(ValueError):
            va.make_label(self.verification, {**self.deployment, "address": b"short"}, self.compilation, {"sourcify": "sourcify"})

    def test_prior_sourcify_values_and_same_batch_duplicates_are_skipped(self):
        existing = {(self.label["chain_id"], self.label["address"], tag, va.json_value(value))
                    for tag, value in self.label["tags"].items()
                    if tag not in ("deployment_block", "_source", "_comment")}
        missing = va.missing_labels([self.label, self.label], existing, self.state)
        self.assertEqual(len(missing), 1)
        self.assertEqual(set(missing[0]["tags"]), {"deployment_block", "source_code_verified", "_source", "_comment"})

    def test_provenance_alone_does_not_trigger_submission(self):
        existing = {(self.label["chain_id"], self.label["address"], tag, va.json_value(value))
                    for tag, value in self.label["tags"].items() if not tag.startswith("_")}
        self.assertEqual(va.missing_labels([self.label], existing, self.state), [])

    def test_rewritten_file_resets_offset_even_if_complete(self):
        file = dict(key="file", etag="old")
        self.state.checkpoint(file, 100, True)
        self.assertEqual(self.state.progress(file), (100, True))
        self.assertEqual(self.state.progress({**file, "etag": "new"}), (0, False))

    def test_timeout_then_partial_acceptance_replays_identical_payload(self):
        signed = [{"uid": "stable-uid"}]
        self.state.db.execute("INSERT INTO outbox VALUES (1, ?, ?)", (json.dumps(signed), json.dumps([self.label])))
        self.state.db.commit()
        client = Mock(tag_definitions=DEFINITIONS)
        client.api.post_bulk_attestations.side_effect = TimeoutError("lost response")
        with self.assertRaises(TimeoutError):
            va.flush_outbox(self.state, client)
        self.assertEqual(self.state.db.execute("SELECT count(*) FROM outbox").fetchone()[0], 1)
        response = Mock()
        response.json.return_value = {"accepted": 0, "duplicates": 0, "failed_validation": [{"index": 0}]}
        client.api.post_bulk_attestations.side_effect = None
        client.api.post_bulk_attestations.return_value = response
        with self.assertRaises(RuntimeError):
            va.flush_outbox(self.state, client)
        response.json.return_value = {"accepted": 0, "duplicates": 1, "failed_validation": []}
        va.flush_outbox(self.state, client)
        self.assertTrue(all(call.args[0] == signed for call in client.api.post_bulk_attestations.call_args_list))
        self.assertEqual(self.state.db.execute("SELECT count(*) FROM outbox").fetchone()[0], 0)
        self.assertEqual(va.missing_labels([self.label], set(), self.state), [])

    def test_paginated_listing_sorts_numeric_ranges(self):
        session = Mock()
        def page(start, truncated, marker=""):
            key = f"v2/verified_contracts/verified_contracts_{start}_{start + 100}.parquet"
            return Mock(content=f'<ListBucketResult xmlns="urn:test"><IsTruncated>{truncated}</IsTruncated>'
                        f'<NextMarker>{marker}</NextMarker><Contents><Key>{key}</Key><ETag>"a"</ETag>'
                        '<Size>42</Size></Contents></ListBucketResult>'.encode())
        session.get.side_effect = [page(100, "true", "next"), page(20, "false")]
        files = va.list_files(session, "verified_contracts")
        self.assertIn("_20_", files[0]["key"])
        self.assertEqual(session.get.call_args.kwargs["params"]["marker"], "next")

    def test_real_parquet_projection_over_pinned_http_ranges(self):
        sink = pa.BufferOutputStream()
        pq.write_table(pa.Table.from_pylist([self.compilation]), sink)
        content = sink.getvalue().to_pybytes()
        session = Mock()
        def get(url, headers, timeout):
            self.assertEqual(headers["If-Match"], '"etag"')
            start, end = map(int, headers["Range"].split("=")[1].split("-"))
            response = Mock(status_code=206, content=content[start:end+1])
            response.__enter__ = Mock(return_value=response)
            response.__exit__ = Mock(return_value=False)
            return response
        session.get.side_effect = get
        rows = list(va.parquet_batches(session, dict(key="test", size=len(content), etag='"etag"'), "compiled_contracts"))
        self.assertEqual(rows, [[self.compilation]])

    def test_dry_run_then_live_then_incremental_skip(self):
        data = {"verified_contracts": self.verification, "contract_deployments": self.deployment,
                "compiled_contracts": self.compilation}
        def listing(session, table):
            return [dict(key=f"v2/{table}/{table}_0_100.parquet", etag="v1", size=10)]
        def batches(session, file, table, batch_size=500):
            yield [data[table]]
        client = Mock(tag_definitions=DEFINITIONS)
        client.api.post_bulk_attestations.return_value.json.return_value = {"accepted": 1, "duplicates": 0}
        with tempfile.TemporaryDirectory() as directory, patch.object(va, "list_files", side_effect=listing), \
                patch.object(va, "parquet_batches", side_effect=batches), \
                patch.object(va, "existing_labels", side_effect=lambda *args: set()), \
                patch.object(va, "sign_labels", return_value=[{"uid": "stable"}]) as sign:
            result = va.sync(directory, Mock(), dry_run=True)
            self.assertEqual(result["attestations"], 1)
            sign.assert_not_called()
            with sqlite_connection(directory) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM files WHERE key LIKE '%verified_contracts%'").fetchone()[0], 0)
            result = va.sync(directory, Mock(), client, dry_run=False)
            self.assertEqual(result["attestations"], 1)
            result = va.sync(directory, Mock(), client, dry_run=False)
            self.assertEqual(result["scanned"], 0)
            client.api.post_bulk_attestations.assert_called_once()

    def test_missing_join_does_not_advance_verification_checkpoint(self):
        def listing(session, table):
            return [dict(key=f"v2/{table}/{table}_0_100.parquet", etag="v1", size=10)]
        def batches(session, file, table, batch_size=500):
            if table == "verified_contracts":
                yield [self.verification]
            else:
                yield []
        with tempfile.TemporaryDirectory() as directory, patch.object(va, "list_files", side_effect=listing), \
                patch.object(va, "parquet_batches", side_effect=batches):
            with self.assertRaisesRegex(RuntimeError, "dependency missing"):
                va.sync(directory, Mock(), Mock(), dry_run=False)
            with sqlite_connection(directory) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM files WHERE key LIKE '%verified_contracts%'").fetchone()[0], 0)

    def test_changed_tail_includes_late_lower_id_without_duplicate_submissions(self):
        version = ["v1"]
        late = {**self.verification, "id": 0, "deployment_id": "late"}
        def listing(session, table):
            return [dict(key=f"v2/{table}/{table}_0_100.parquet", etag=version[0], size=10)]
        def batches(session, file, table, batch_size=500):
            if table == "verified_contracts":
                yield [self.verification] + ([late] if version[0] == "v2" else [])
            elif table == "contract_deployments":
                yield [self.deployment, {**self.deployment, "id": "late", "address": b"\x78" * 20}]
            else:
                yield [self.compilation]
        client = Mock(tag_definitions=DEFINITIONS)
        client.api.post_bulk_attestations.return_value.json.return_value = {"accepted": 1, "duplicates": 0}
        with tempfile.TemporaryDirectory() as directory, patch.object(va, "list_files", side_effect=listing), \
                patch.object(va, "parquet_batches", side_effect=batches), \
                patch.object(va, "existing_labels", side_effect=lambda *args: set()), \
                patch.object(va, "sign_labels", return_value=[{"uid": "signed"}]):
            va.sync(directory, Mock(), client, dry_run=False)
            version[0] = "v2"
            result = va.sync(directory, Mock(), client, dry_run=False)
            self.assertEqual(result["scanned"], 2)
            self.assertEqual(result["attestations"], 1)
            self.assertEqual(client.api.post_bulk_attestations.call_count, 2)

    def test_signing_rejects_schema_violation_before_outbox(self):
        client = Mock(tag_definitions={**DEFINITIONS, "is_contract": {"schema": {"type": "boolean"}}})
        with self.assertRaises(ValidationError):
            va.sign_labels(client, [{**self.label, "tags": {"is_contract": "true", "source_code_verified": "sourcify"}}])
        client.offchain.build_offchain_attestation.assert_not_called()

    def test_checksum_case_does_not_create_a_new_deployer_label(self):
        self.assertEqual(va.json_value("0xAbCd", "deployer_address"), "0xabcd")

    def test_unknown_role_alerts_and_blocks_before_cache_or_submission(self):
        file = dict(key="v2/verified_contracts/test.parquet", etag="one")
        notify = Mock()
        with tempfile.TemporaryDirectory() as directory, patch.object(va, "list_files", return_value=[file]), \
                patch.object(va, "verifier_roles", return_value={"routescan"}), \
                patch.object(va, "parquet_batches") as read, patch.object(va, "sign_labels") as sign:
            for _ in range(2):
                with self.assertRaisesRegex(va.UnknownVerifierError, "routescan"):
                    va.sync(directory, Mock(), Mock(), dry_run=False, notify=notify)
            read.assert_not_called()
            sign.assert_not_called()
            notify.assert_called_once()
            self.assertIn("routescan", notify.call_args.args[0])
            with sqlite_connection(directory) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM files").fetchone()[0], 0)

    def test_new_schema_entity_unblocks_cached_roles_without_code_changes(self):
        file = dict(key="file", etag="v1")
        with patch.object(va, "verifier_roles", return_value={"routescan"}) as roles:
            with self.assertRaises(va.UnknownVerifierError):
                va.preflight_verifiers(self.state, Mock(), [file], {"sourcify"}, {})
            mapping = va.preflight_verifiers(self.state, Mock(), [file], {"sourcify", "routescan"}, {})
            self.assertEqual(mapping, {"routescan": "routescan"})
            roles.assert_called_once()

    def test_failed_discord_delivery_is_retried(self):
        notify = Mock(side_effect=RuntimeError("Discord unavailable"))
        with self.assertRaisesRegex(RuntimeError, "Discord"):
            va.notify_once(self.state, "test", "unknown verifier", notify)
        notify.side_effect = None
        va.notify_once(self.state, "test", "unknown verifier", notify)
        self.assertEqual(notify.call_count, 2)

    def test_dry_run_unknown_role_never_sends_discord(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(va, "list_files", return_value=[dict(key="file", etag="1")]), \
                patch.object(va, "verifier_roles", return_value={"routescan"}):
            notify = Mock()
            with self.assertRaises(va.UnknownVerifierError):
                va.sync(directory, Mock(), dry_run=True, notify=notify)
            notify.assert_not_called()

    def test_receipts_removed_only_after_exact_values_visible_in_oli(self):
        rows = [("eip155:1", self.label["address"], "contract_name", "Token"),
                ("eip155:1", self.label["address"], "code_language", "solidity")]
        with self.state.db:
            self.state.db.executemany("INSERT INTO sent VALUES (?, ?, ?, ?)", rows)
        with patch.object(va, "existing_labels", return_value=set()):
            self.assertEqual(va.reconcile_receipts(self.state, Mock(), limit=1), 0)
        with patch.object(va, "existing_labels", return_value={rows[1]}):
            self.assertEqual(va.reconcile_receipts(self.state, Mock(), limit=1), 1)
        self.assertEqual(self.state.db.execute("SELECT chain,address,tag,value FROM sent").fetchall(), [rows[0]])
        with patch.object(va, "existing_labels", return_value={rows[0]}):
            self.assertEqual(va.reconcile_receipts(self.state, Mock(), limit=1), 1)

    def test_reconciliation_failure_keeps_receipts(self):
        with self.state.db:
            self.state.db.execute("INSERT INTO sent VALUES ('eip155:1', ?, 'is_contract', 'true')", (self.label["address"],))
        with patch.object(va, "existing_labels", side_effect=RuntimeError("OLI offline")):
            with self.assertRaises(RuntimeError):
                va.reconcile_receipts(self.state, Mock())
        self.assertEqual(self.state.db.execute("SELECT count(*) FROM sent").fetchone()[0], 1)

    def test_sqlite_enforces_size_cap_and_retains_committed_outbox(self):
        with self.state.db:
            self.state.db.execute("INSERT INTO outbox VALUES (1, 'signed', 'labels')")
        pages = self.state.db.execute("PRAGMA page_count").fetchone()[0]
        self.state.db.execute(f"PRAGMA max_page_count={pages + 1}")
        with self.assertRaises(sqlite3.DatabaseError) as exc:
            with self.state.db:
                self.state.db.execute("INSERT INTO dimensions VALUES ('test','id',?)", ('x' * 1000000,))
        self.assertEqual(exc.exception.sqlite_errorcode, sqlite3.SQLITE_FULL)
        self.assertEqual(self.state.db.execute("SELECT payload FROM outbox").fetchone()[0], "signed")
        self.assertLessEqual(self.state.db.execute("PRAGMA page_count").fetchone()[0], pages + 1)

    def test_cannot_configure_database_above_90_gb(self):
        with self.assertRaises(ValueError):
            va.State(":memory:", max_bytes=100_000_000_000)

    def test_legacy_outbox_without_source_is_not_sent(self):
        label = {**self.label, "tags": {"contract_name": "Token"}}
        with self.state.db:
            self.state.db.execute("INSERT INTO outbox VALUES (1, '[]', ?)", (json.dumps([label]),))
        oli = Mock(tag_definitions=DEFINITIONS)
        with self.assertRaises(va.UnknownVerifierError):
            va.flush_outbox(self.state, oli)
        oli.api.post_bulk_attestations.assert_not_called()

    def test_every_new_attestation_carries_source_even_when_already_in_oli(self):
        existing = {(self.label['chain_id'], self.label['address'], 'source_code_verified', 'sourcify')}
        pending = va.missing_labels([self.label], existing, self.state)
        self.assertEqual(pending[0]['tags']['source_code_verified'], 'sourcify')

    def test_capacity_failure_alerts_and_does_not_report_success(self):
        error = sqlite3.OperationalError("database or disk is full")
        error.sqlite_errorcode = sqlite3.SQLITE_FULL
        notify = Mock()
        with patch.object(va, "_sync", side_effect=error):
            with self.assertRaises(sqlite3.OperationalError):
                va.sync("unused", Mock(), Mock(), dry_run=False, notify=notify)
        notify.assert_called_once()
        self.assertIn("90 GB", notify.call_args.args[0])


def sqlite_connection(directory):
    import sqlite3
    return sqlite3.connect(Path(directory) / "state.sqlite")


if __name__ == "__main__":
    unittest.main()
