"""Write-path validation of the OLI Label Pool API (UID, EAS domain/schema, time, label subject).

PYTHONPATH=backend python -m unittest discover -s backend/tests -p test_oli_api_validation.py
Payloads are built and signed like oli-python's build_offchain_attestation, with a throwaway key.
"""

import copy
import json
import os
import secrets
import time as _time
import unittest
from unittest.mock import patch

from eth_abi import encode as abi_encode
from eth_account import Account
from fastapi import HTTPException

TEST_ENV = {"OLI_KEY_PEPPER": "test-pepper", "OLI_ADMIN_BEARER": "test-admin",
            "OLI_PRIVATE_ATTESTERS": "0x" + "11" * 20}
with patch.dict(os.environ, TEST_ENV):
    from src.oli.api import api

LABEL_SCHEMA_V2 = "0xcff83309b59685fdae9dad7c63d969150676d51d8eeda66799d1c4898b84556a"
LABEL_SCHEMA_V1 = "0xb763e62d940bed6f527dd82418e146a904e62a297b8fa765c9b3e1f0bc6fdd68"
EAS_BASE = "0x4200000000000000000000000000000000000021"
ZERO32 = "0x" + "00" * 32
ATTEST_TYPES = [{"name": n, "type": t} for n, t in api.EAS_ATTEST_TYPES]
ACCOUNT = Account.create()


def encode_data(subject, tags):
    tags_str = tags if isinstance(tags, str) else json.dumps(tags)
    return "0x" + abi_encode(["string", "string"], [subject, tags_str]).hex()


def build_payload(subject="eip155:8453:0x3Ae5F83668B75328446c649B6ab342aC46D73B3c",
                  tags=None, schema=LABEL_SCHEMA_V2, domain=None, types=None, account=ACCOUNT, time=1762761600):
    """Signed payload as oli-python builds it; uid computed by an independent copy of the EAS v2 rule."""
    tags = {"contract_name": "Test", "usage_category": "dex"} if tags is None else tags
    domain = domain or {"name": "EAS Attestation", "version": "1.2.0", "chainId": 8453, "verifyingContract": EAS_BASE}
    types = types or ATTEST_TYPES
    message = {
        "version": 2, "recipient": "0x0000000000000000000000000000000000000001", "time": time,
        "revocable": True, "schema": schema, "refUID": ZERO32, "data": encode_data(subject, tags),
        "expirationTime": 0, "salt": "0x" + secrets.token_hex(32),
    }
    signed = account.sign_typed_data(domain_data=domain, message_types={"Attest": types}, message_data=message)
    return {
        "sig": {
            "domain": {**domain, "chainId": str(domain["chainId"])},
            "primaryType": "Attest",
            "types": {"Attest": types},
            "message": {**message, "time": str(message["time"]), "expirationTime": str(message["expirationTime"])},
            "uid": sdk_uid(message),
            "version": 2,
            "signature": {"r": hex(signed.r), "s": hex(signed.s), "v": signed.v},
        },
        "signer": account.address,
    }


def sdk_uid(m):
    from web3 import Web3
    packed = (
        (2).to_bytes(2, "big") + m["schema"].encode() + Web3.to_bytes(hexstr=m["recipient"]) + b"\x00" * 20
        + m["time"].to_bytes(8, "big") + m["expirationTime"].to_bytes(8, "big") + b"\x01"
        + Web3.to_bytes(hexstr=m["refUID"]) + Web3.to_bytes(hexstr=m["data"]) + Web3.to_bytes(hexstr=m["salt"])
        + (0).to_bytes(4, "big")
    )
    return "0x" + Web3.keccak(packed).hex().removeprefix("0x")


def check(raw, schemas=None):
    payload = api.AttestationPayload.model_validate(raw)
    api.verify_attestation(payload, schemas or api.LABEL_POOL_SCHEMAS)
    return api.build_row_from_payload(payload)


class AcceptedPayloadTests(unittest.TestCase):
    def test_sdk_style_label_is_accepted(self):
        row = check(build_payload())
        self.assertEqual(row["chain_id"], "eip155:8453")
        self.assertEqual(row["recipient"], "0x3ae5f83668b75328446c649b6ab342ac46d73b3c")

    def test_v1_schema_with_chain_id_subject_is_accepted(self):
        row = check(build_payload(subject="eip155:10", schema=LABEL_SCHEMA_V1))
        self.assertEqual(row["chain_id"], "eip155:10")

    def test_non_evm_caip10_is_accepted(self):
        addr = "0x0" + "a" * 63
        row = check(build_payload(subject=f"starknet:SN_MAIN:{addr}"))
        self.assertEqual(row["chain_id"], "starknet:SN_MAIN")

    def test_eas_domain_version_1_0_1_is_accepted(self):
        domain = {"name": "EAS Attestation", "version": "1.0.1", "chainId": 8453, "verifyingContract": EAS_BASE}
        check(build_payload(domain=domain))

    def test_small_clock_skew_is_accepted(self):
        check(build_payload(time=int(_time.time()) + 60))

    def test_uid_matches_oli_python_sdk(self):
        try:
            from oli.attestation.utils_other import UtilsOther
        except ImportError:
            self.skipTest("oli-python not installed")
        raw = build_payload()
        m = raw["sig"]["message"]
        sdk = UtilsOther(None).calculate_attestation_uid_v2(
            m["schema"], m["recipient"], "0x" + "00" * 20, int(m["time"]), m["data"],
            int(m["expirationTime"]), m["revocable"], m["refUID"], salt=m["salt"])
        msg = api.AttestationPayload.model_validate(raw).sig.message
        self.assertEqual(api.compute_offchain_uid(msg), bytes(sdk))


class RejectedPayloadTests(unittest.TestCase):
    def assertRejected(self, raw, fragment, schemas=None):
        with self.assertRaises(HTTPException) as ctx:
            check(raw, schemas)
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn(fragment, ctx.exception.detail)

    # 1. uid
    def test_client_chosen_uid_is_rejected(self):
        raw = build_payload()
        raw["sig"]["uid"] = "0x" + "ab" * 32
        self.assertRejected(raw, "uid does not match")

    def test_uid_of_another_attestation_is_rejected(self):
        victim, raw = build_payload(), build_payload()
        raw["sig"]["uid"] = victim["sig"]["uid"]
        self.assertRejected(raw, "uid does not match")

    # 2. domain / schema / types
    def test_foreign_schema_is_rejected(self):
        self.assertRejected(build_payload(schema="0x" + "cd" * 32), "Unsupported schema")

    def test_trust_list_schema_rejected_on_label_endpoint_and_vice_versa(self):
        self.assertRejected(build_payload(schema=api.TRUST_LIST_SCHEMA), "Unsupported schema")
        self.assertRejected(build_payload(), "Unsupported schema", schemas={api.TRUST_LIST_SCHEMA})

    def test_other_chain_domain_is_rejected(self):
        domain = {"name": "EAS Attestation", "version": "1.2.0", "chainId": 42161,
                  "verifyingContract": "0xbD75f629A22Dc1ceD33dDA0b68c546A1c035c458"}
        self.assertRejected(build_payload(domain=domain), "Unsupported domain.chainId")

    def test_wrong_verifying_contract_is_rejected(self):
        domain = {"name": "EAS Attestation", "version": "1.2.0", "chainId": 8453,
                  "verifyingContract": "0x" + "12" * 20}
        self.assertRejected(build_payload(domain=domain), "verifyingContract")

    def test_wrong_domain_name_is_rejected(self):
        domain = {"name": "Not EAS", "version": "1.2.0", "chainId": 8453, "verifyingContract": EAS_BASE}
        self.assertRejected(build_payload(domain=domain), "Unsupported EIP-712 domain")

    def test_non_standard_types_are_rejected(self):
        types = [t for t in ATTEST_TYPES if t["name"] != "salt"] + [{"name": "salt", "type": "bytes32"}, {"name": "x", "type": "uint8"}]
        raw = build_payload()
        raw["sig"]["types"]["Attest"] = types
        self.assertRejected(raw, "Non-standard Attest types")

    # 3. time
    def test_future_time_is_rejected(self):
        self.assertRejected(build_payload(time=2109656004), "in the future")  # 2036-11-07, as in the probe
        self.assertRejected(build_payload(time=int(_time.time()) + 6 * 60), "in the future")

    # 4. subject / tags
    def test_invalid_evm_address_is_rejected(self):
        self.assertRejected(build_payload(subject="eip155:8453:0xdead"), "Invalid EVM address")

    def test_invalid_chain_id_is_rejected(self):
        self.assertRejected(build_payload(subject="eip155:base:0x" + "33" * 20), "Invalid eip155 chain reference")
        self.assertRejected(build_payload(subject="EIP 155:8453:0x" + "33" * 20), "Invalid chain_id")

    def test_unparseable_or_empty_tags_are_rejected(self):
        self.assertRejected(build_payload(tags="{not json"), "tags_json")
        self.assertRejected(build_payload(tags={}), "tags_json")
        self.assertRejected(build_payload(tags=["dex"]), "tags_json")

    def test_tampered_message_breaks_signature_not_uid_only(self):
        raw = build_payload()
        raw["sig"]["message"]["data"] = encode_data("eip155:8453:0x" + "44" * 20, {"contract_name": "x"})
        self.assertRejected(raw, "uid does not match")


if __name__ == "__main__":
    unittest.main()
