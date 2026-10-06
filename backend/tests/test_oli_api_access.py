"""Privacy and quota regressions. Optional integration DB must be isolated.

PYTHONPATH=backend python -m unittest discover -s backend/tests -p test_oli_api_access.py
Set OLI_TEST_DSN to a localhost PostgreSQL database named oli_api_test to also
exercise real SQL, parallel reservations, and HTTP routes. Never uses backend/.env.
"""

import asyncio
import json
import os
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch
from urllib.parse import urlparse

import asyncpg
import httpx
from fastapi import HTTPException

PRIVATE = "0x" + "11" * 20
PUBLIC = "0x" + "22" * 20
ADDRESS = "0x" + "33" * 20
TEST_ENV = {"OLI_KEY_PEPPER": "test-pepper", "OLI_ADMIN_BEARER": "test-admin",
            "OLI_PRIVATE_ATTESTERS": PRIVATE}
with patch.dict(os.environ, TEST_ENV):
    from src.oli.api import api
from src.oli.api import oli_private_attesters as private
from src.oli.api.oli_access import ReadAccess, ReadLimits
from src.oli.api.oli_public_cleanup import remove_private_analytics_files


class PrivacyConfigurationTests(unittest.TestCase):
    def tearDown(self):
        private.reset_private_attester_cache()

    def test_absent_empty_or_invalid_configuration_fails_closed(self):
        for value in ["", "{}", "not-an-address"]:
            with self.subTest(value=value), patch.dict(os.environ, {"OLI_PRIVATE_ATTESTERS": value}):
                private.reset_private_attester_cache()
                with self.assertRaises((RuntimeError, ValueError)):
                    private.private_attester_exclusion_sql()
                with self.assertRaises((RuntimeError, ValueError)):
                    api.add_private_attester_exclusion([], [], 1)

    def test_cleanup_deletes_private_objects_and_invalidates_exact_paths(self):
        with patch.dict(os.environ, TEST_ENV):
            private.reset_private_attester_cache()
            s3, cf = Mock(), Mock()
            cf.create_invalidation.return_value = {"Invalidation": {"Id": "test"}}
            self.assertEqual(remove_private_analytics_files(s3, cf, "bucket", "distribution"), "test")
            key = f"v1/oli/analytics/attester/{PRIVATE[2:]}.json"
            s3.delete_object.assert_called_once_with(Bucket="bucket", Key=key)
            paths = cf.create_invalidation.call_args.kwargs["InvalidationBatch"]["Paths"]
            self.assertEqual(paths, {"Quantity": 1, "Items": ["/" + key]})

    def test_cleanup_does_nothing_with_missing_private_configuration(self):
        with patch.dict(os.environ, {"OLI_PRIVATE_ATTESTERS": ""}):
            private.reset_private_attester_cache()
            s3, cf = Mock(), Mock()
            with self.assertRaises(RuntimeError):
                remove_private_analytics_files(s3, cf, "bucket", "distribution")
            s3.delete_object.assert_not_called()
            cf.create_invalidation.assert_not_called()

    def test_private_address_formats_normalize_to_same_exclusion(self):
        variants = [PRIVATE.upper().replace("0X", "0x"), "\\x" + PRIVATE[2:], json.dumps({PRIVATE: "private"})]
        for value in variants:
            with self.subTest(value=value), patch.dict(os.environ, {"OLI_PRIVATE_ATTESTERS": value}):
                private.reset_private_attester_cache()
                self.assertEqual(private.require_private_attesters(), [bytes.fromhex(PRIVATE[2:])])

    def test_frozen_sample_is_capped_balanced_and_has_deployment_labels(self):
        samples = api.load_curated_sample()
        self.assertEqual(len(samples), 100)
        self.assertEqual(set(Counter(s.chain_id for s in samples).values()), {20})
        self.assertEqual(len({(s.chain_id, s.address) for s in samples}), 100)
        self.assertTrue(any("deployment_tx" in s.tags for s in samples))

    def test_sample_rejects_oversized_or_duplicate_sets(self):
        rows = json.loads(Path(api.__file__).with_name("curated_sample.json").read_text())
        for values in [rows + [rows[0]], [rows[0], rows[0]]]:
            with tempfile.TemporaryDirectory() as directory:
                sample_path = Path(directory) / "sample.json"
                sample_path.write_text(json.dumps(values))
                with patch.dict(os.environ, {"OLI_CURATED_SAMPLE_FILE": str(sample_path)}), self.assertRaises(ValueError):
                    api.load_curated_sample()


DSN = os.getenv("OLI_TEST_DSN")


class StartupPrivacyTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_private_configuration_stops_before_database_connection(self):
        with patch.dict(os.environ, {"OLI_PRIVATE_ATTESTERS": ""}), patch.object(api.asyncpg, "create_pool", new_callable=AsyncMock) as create_pool:
            private.reset_private_attester_cache()
            try:
                with self.assertRaises(RuntimeError):
                    async with api.lifespan(api.app):
                        self.fail("Startup unexpectedly succeeded")
                create_pool.assert_not_called()
            finally:
                private.reset_private_attester_cache()


@unittest.skipUnless(DSN, "Set OLI_TEST_DSN for isolated PostgreSQL integration tests")
class ReadAccessIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        parsed = urlparse(DSN)
        if parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.path != "/oli_api_test":
            raise RuntimeError("OLI_TEST_DSN must target localhost/oli_api_test")
        self.env = patch.dict(os.environ, TEST_ENV)
        self.env.start()
        private.reset_private_attester_cache()
        self.pool = await asyncpg.create_pool(DSN, min_size=1, max_size=10, command_timeout=10)
        async with self.pool.acquire() as conn:
            await conn.execute(Path(api.__file__).with_name("migrations").joinpath("001_read_limits.sql").read_text())
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS public.api_keys (
                    id uuid PRIMARY KEY, owner_id text, prefix text, key_hash text, revoked_at timestamptz,
                    usage_count int DEFAULT 0, last_used_at timestamptz);
                CREATE TABLE IF NOT EXISTS public.api_key_usage (
                    key_id uuid, endpoint text, status_code int, ip inet);
                CREATE TABLE IF NOT EXISTS public.labels (
                    uid bytea, address text, chain_id text, tag_id text, tag_value text,
                    time timestamptz, attester bytea);
                CREATE TABLE IF NOT EXISTS public.attestations (
                    uid bytea, time timestamptz, chain_id text, attester bytea, recipient text,
                    revoked bool, is_offchain bool, ipfs_hash text, schema_info text, tags_json jsonb);
                CREATE TABLE IF NOT EXISTS public.trust_lists (
                    uid bytea, time timestamptz, attester bytea, recipient text, revoked bool,
                    is_offchain bool, tx_hash bytea, ipfs_hash text, revocation_time timestamptz,
                    raw jsonb, last_updated_time timestamptz, schema_info text, owner_name text,
                    attesters jsonb, attestations jsonb);
                TRUNCATE public.api_keys, public.api_key_usage, public.labels, public.attestations,
                         public.trust_lists, public.api_read_buckets, public.api_read_leases;
            """)
            self.keys = []
            for i in [1, 2]:
                key, prefix, key_hash = api.make_key()
                self.keys.append(key)
                await conn.execute("INSERT INTO public.api_keys(id, owner_id, prefix, key_hash) VALUES ($1::uuid, 'same-account', $2, $3)",
                                   f"00000000-0000-0000-0000-00000000000{i}", prefix, key_hash)
            for a, value, uid in [(PRIVATE, "secret", b"\x01" * 32), (PUBLIC, "community", b"\x02" * 32)]:
                await conn.execute("INSERT INTO public.labels VALUES ($1, $2, 'eip155:1', 'contract_name', $3, now(), $4)",
                                   uid, ADDRESS, value, bytes.fromhex(a[2:]))
                await conn.execute("""INSERT INTO public.attestations VALUES
                    ($1, now(), 'eip155:1', $2, $3, false, true, NULL, 'schema', $4::jsonb)""",
                                   uid, bytes.fromhex(a[2:]), ADDRESS, json.dumps({"contract_name": value}))
                await conn.execute("""INSERT INTO public.trust_lists(uid, time, attester, owner_name, revoked, is_offchain)
                    VALUES ($1, now(), $2, $3, false, true)""", uid, bytes.fromhex(a[2:]), value)
        api.app.state.db = self.pool
        api.app.state.read_access = ReadAccess(self.pool, ReadLimits(60, 10000, 100, 5))
        api.app.state.curated_sample = api.load_curated_sample()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=api.app), base_url="http://test",
                                        headers={"x-api-key": self.keys[0]})

    async def asyncTearDown(self):
        await self.client.aclose()
        await self.pool.close()
        self.env.stop()
        private.reset_private_attester_cache()

    async def test_all_read_paths_exclude_private_mappings(self):
        paths = [f"/labels?address={ADDRESS}", f"/labels?address={ADDRESS}&include_all=true&chain_id=eip155:1",
                 "/addresses/search?tag_id=contract_name", "/addresses/search?tag_id=contract_name&tag_value=secret&chain_id=eip155:1",
                 "/analytics/attesters", "/analytics/attesters?chain_id=eip155:1", "/attestations",
                 f"/attestations?attester={PRIVATE}", "/attestations?uid=0x" + "01" * 32,
                 "/trust-lists", f"/trust-lists?attester={PRIVATE}", "/trust-lists?uid=0x" + "01" * 32]
        for path in paths:
            with self.subTest(path=path):
                response = await self.client.get(path)
                self.assertEqual(response.status_code, 200, response.text)
                if "tag_value=secret" in path:
                    self.assertEqual(response.json()["results"], [])
                else:
                    self.assertNotIn("secret", response.text)
                self.assertNotIn(PRIVATE, response.text)
        for include_all in [True, False]:
            response = await self.client.post("/labels/bulk", json={"addresses": [ADDRESS], "include_all": include_all,
                                                                       "chain_id": "eip155:1"})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["results"][0]["labels"][0]["tag_value"], "community")
            self.assertNotIn(PRIVATE, response.text)

    async def test_protected_reads_require_valid_keys_and_do_not_attribute_forged_keys(self):
        for path in ["/trust-lists", "/labels?address=" + ADDRESS]:
            response = await self.client.get(path, headers={"x-api-key": ""})
            self.assertEqual(response.status_code, 401)
        forged = self.keys[0].split(".")[0] + ".wrong-secret"
        response = await self.client.get("/plans", headers={"x-api-key": forged})
        self.assertEqual(response.status_code, 200)
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT sum(usage_count) FROM public.api_keys"), 0)

    async def test_public_attestations_exclude_private_rows_without_account_charges(self):
        paths = ["/attestations", f"/attestations?attester={PRIVATE}",
                 "/attestations?uid=0x" + "01" * 32, "/attestations?uid=0x" + "02" * 32]
        for key in ["", "invalid-key", self.keys[0]]:
            for path in paths:
                with self.subTest(key_present=bool(key), path=path):
                    response = await self.client.get(path, headers={"x-api-key": key})
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertNotIn("secret", response.text)
                    self.assertNotIn(PRIVATE, response.text)
                    expected_count = 0 if PRIVATE in path or "01" * 32 in path else 1
                    self.assertEqual(response.json()["count"], expected_count)
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM public.api_read_buckets"), 0)
            self.assertEqual(await conn.fetchval("SELECT sum(usage_count) FROM public.api_keys"), 0)

    async def test_monthly_quota_is_shared_across_keys_and_usage_remains_accessible(self):
        api.app.state.read_access = ReadAccess(self.pool, ReadLimits(60, 1, 100, 5))
        first = await self.client.get("/labels?address=" + ADDRESS)
        second = await self.client.get("/labels?address=" + ADDRESS, headers={"x-api-key": self.keys[1]})
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 429, second.text)
        self.assertEqual(second.json()["detail"]["code"], "monthly_address_quota")
        usage = await self.client.get("/account/usage")
        self.assertEqual(usage.status_code, 200)
        self.assertEqual(usage.json()["address_slots_used"], 1)

    async def test_rate_rejections_are_committed_and_failures_refund_address_slots(self):
        access = ReadAccess(self.pool, ReadLimits(1, 10000, 100, 5))
        first = await access.reserve("account", 10)
        await access.release(first, failed=True)
        for _ in range(2):
            with self.assertRaises(HTTPException) as error:
                await access.reserve("account", 1)
            self.assertEqual(error.exception.detail["code"], "rate_limit")
        self.assertEqual((await access.usage("account"))["address_slots_used"], 0)
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT used FROM public.api_read_buckets WHERE owner_id='account' AND kind='minute'"), 3)

    async def test_parallel_workers_cannot_exceed_account_concurrency_or_monthly_quota(self):
        access = ReadAccess(self.pool, ReadLimits(100, 100, 100, 2))
        other_worker = ReadAccess(self.pool, access.limits)
        results = await asyncio.gather(*(worker.reserve("parallel", 1) for worker in [access, other_worker] * 5), return_exceptions=True)
        accepted = [result for result in results if not isinstance(result, Exception)]
        self.assertEqual(len(accepted), 2)
        self.assertTrue(all(result.detail["code"] == "concurrent_read_limit" for result in results if isinstance(result, HTTPException)))
        for result in accepted:
            await access.release(result)
        quota = ReadAccess(self.pool, ReadLimits(100, 2, 100, 20))
        results = await asyncio.gather(*(quota.reserve("quota-parallel", 1) for _ in range(10)), return_exceptions=True)
        accepted = [result for result in results if not isinstance(result, Exception)]
        self.assertEqual(len(accepted), 2)
        self.assertEqual((await quota.usage("quota-parallel"))["address_slots_used"], 2)
        for result in accepted:
            await quota.release(result)

    async def test_expired_leases_recover_and_release_is_idempotent(self):
        access = ReadAccess(self.pool, ReadLimits(100, 100, 100, 1))
        first = await access.reserve("crashed", 10)
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE public.api_read_leases SET expires_at=now()-interval '1 second'")
        second = await access.reserve("crashed", 1)
        await access.release(second, failed=True)
        await access.release(second, failed=True)
        self.assertEqual((await access.usage("crashed"))["address_slots_used"], 10)

    async def test_bulk_deduplication_validation_and_fixed_trial(self):
        response = await self.client.post("/labels/bulk", json={"addresses": [ADDRESS, ADDRESS.upper().replace("0X", "0x")]})
        self.assertEqual(response.status_code, 200, response.text)
        usage = await self.client.get("/account/usage")
        self.assertEqual(usage.json()["address_slots_used"], 1)
        response = await self.client.post("/labels/bulk", json={"addresses": [ADDRESS] * 101})
        self.assertEqual(response.status_code, 422)
        response = await self.client.get("/curated/sample?address=" + ADDRESS)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()["samples"]), 100)
        self.assertNotIn("secret", response.text)

    async def test_key_revocation_takes_effect_on_next_request(self):
        self.assertEqual((await self.client.get("/account/usage")).status_code, 200)
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE public.api_keys SET revoked_at=now()")
        self.assertEqual((await self.client.get("/account/usage")).status_code, 401)

    async def test_http_validation_failure_refunds_reserved_address_slots(self):
        response = await self.client.get("/labels", params={"address": ADDRESS, "include_all": "not-a-boolean"})
        self.assertEqual(response.status_code, 422)
        self.assertEqual((await api.app.state.read_access.usage("same-account"))["address_slots_used"], 0)
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM public.api_read_leases"), 0)


if __name__ == "__main__":
    unittest.main()
