import os, json, hashlib, asyncpg, time, asyncio, base64, secrets
from fastapi import FastAPI, HTTPException, Query, Depends, Security, status, Request, Header
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Any, Dict, Tuple, Literal
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from collections import OrderedDict

from eth_utils import to_normalized_address
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_abi import decode as abi_decode
from concurrent.futures import ProcessPoolExecutor

# from oli import OLI
# from oli.data.trust import UtilsTrust

# oli = OLI(
#     api_key=os.getenv("OLI_API_KEY")
# )

# tune max_workers to match available CPUs in Cloud Run
process_pool = ProcessPoolExecutor(max_workers=4)

USE_DOTENV = os.getenv("USE_DOTENV", "false").lower() == "true"
if USE_DOTENV:
    import dotenv
    dotenv.load_dotenv()
    
# Environment variables
API_KEY_PEPPER = os.getenv("OLI_KEY_PEPPER")   
ADMIN_BEARER = os.getenv("OLI_ADMIN_BEARER")
db_user = os.getenv("DB_USERNAME")
db_passwd = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = 'oli'

if not API_KEY_PEPPER:
    raise RuntimeError("OLI_KEY_PEPPER env var must be set")
if not ADMIN_BEARER:
    raise RuntimeError("OLI_ADMIN_BEARER env var must be set")

#
#   API KEY Setup
#

API_KEY_HEADER = "x-api-key"
api_key_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)

## Key generation / hashing
API_KEY_PREFIX_NS = "oli_" 

def _rand_b32(nbytes: int) -> str:
    # URL-safe, no padding, short and neat
    return base64.urlsafe_b64encode(secrets.token_bytes(nbytes)).decode().rstrip("=")

def make_key() -> tuple[str, str, str]:
    """
    Returns (display_key, prefix, key_hash).
    display_key is what you show the user ONCE.
    """
    prefix = _rand_b32(9)   # ~12 chars
    secret = _rand_b32(24)  # ~32-33 chars
    display = f"{API_KEY_PREFIX_NS}{prefix}.{secret}"
    key_hash = hashlib.sha256((f"{prefix}.{secret}{API_KEY_PEPPER}").encode()).hexdigest()
    return display, prefix, key_hash

def hash_presented_key(presented: str) -> Optional[tuple[str, str]]:
    if not presented.startswith(API_KEY_PREFIX_NS):
        return None
    try:
        rest = presented[len(API_KEY_PREFIX_NS):]
        prefix, secret = rest.split(".", 1)
    except ValueError:
        return None
    h = hashlib.sha256((f"{prefix}.{secret}{API_KEY_PEPPER}").encode()).hexdigest()
    return prefix, h

def consteq(a: str, b: str) -> bool:
    if len(a) != len(b): return False
    res = 0
    for x, y in zip(a.encode(), b.encode()):
        res |= x ^ y
    return res == 0

## Key validation against configured keys
class ApiKeyMeta(BaseModel):
    id: str
    owner_id: str
    prefix: str

async def get_api_key(
    request: Request,
    api_key: Optional[str] = Security(api_key_header),
) -> ApiKeyMeta:
    if not api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing API key - Please provide x-api-key header")

    parsed = hash_presented_key(api_key)
    if not parsed:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Bad API key format - Please provide a valid x-api-key header")

    prefix, presented_hash = parsed

    async with request.app.state.db.acquire() as conn:
        row = await _lookup_key_by_prefix(conn, prefix)
    if not row or row["revoked_at"] is not None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or revoked API key")

    stored_hash = row["key_hash"]
    if not consteq(stored_hash, presented_hash):
        # do not reveal which part failed
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or revoked API key")

    request.state.api_key_id = str(row["id"])
    request.state.api_key_prefix = row["prefix"]
    request.state.api_key_owner = row["owner_id"]
    return ApiKeyMeta(id=str(row["id"]), owner_id=row["owner_id"], prefix=row["prefix"])


## Simple in-memory LRU cache with TTL for API key lookups
class LRUCacheTTL:
    def __init__(self, maxsize=20000, ttl=120):
        self.maxsize, self.ttl = maxsize, ttl
        self.data = OrderedDict()
    def get(self, k):
        v = self.data.get(k)
        if not v: return None
        exp, val = v
        if exp < time.time():
            self.data.pop(k, None); return None
        self.data.move_to_end(k); return val
    def set(self, k, val):
        self.data[k] = (time.time() + self.ttl, val); self.data.move_to_end(k)
        if len(self.data) > self.maxsize: self.data.popitem(last=False)

_key_cache = LRUCacheTTL()
_neg_cache = LRUCacheTTL(ttl=5, maxsize=50000)

async def _lookup_key_by_prefix(conn, prefix:str):
    if _neg_cache.get(prefix) is True: return None
    cached = _key_cache.get(prefix)
    if cached is not None: return cached
    row = await conn.fetchrow("""
        SELECT id, owner_id, prefix, key_hash, revoked_at
          FROM public.api_keys WHERE prefix = $1
    """, prefix)
    if not row:
        _neg_cache.set(prefix, True); return None
    rec = dict(row); _key_cache.set(prefix, rec); return rec

#
# DB CONFIG
#


DB_DSN = f"postgresql://{db_user}:{db_passwd}@{db_host}/{db_name}"

#
# UTILS
#

def hex_to_bytes(value: Optional[str]) -> Optional[bytes]:
    if not value:
        return None
    v = value.lower()
    if v.startswith("0x"):
        v = v[2:]
    if len(v) % 2 != 0:
        v = "0" + v
    return bytes.fromhex(v)

def unix_str_to_datetime(value: str) -> datetime:
    ts_int = int(value)
    return datetime.fromtimestamp(ts_int, tz=timezone.utc)

#
# MODELS (Pydantic v2 style)
#
# Note: We rename `schema` in the message to `schema_id`
#       to avoid "shadows BaseModel.schema" warning.
#       We will still store it in DB as schema_info = message.schema_id.


class AttestationMessage(BaseModel):
    version: int
    schema_id: str = Field(alias="schema")
    recipient: str
    time: str
    expirationTime: str
    revocable: bool
    refUID: str
    data: str
    salt: str


class AttestationDomain(BaseModel):
    name: str
    chainId: str
    version: str
    verifyingContract: str


class AttestationTypesField(BaseModel):
    name: str
    type: str


class AttestationTypes(BaseModel):
    Attest: List[AttestationTypesField]


class AttestationSignatureFields(BaseModel):
    r: str
    s: str
    v: int


class AttestationSig(BaseModel):
    uid: str
    types: AttestationTypes
    domain: AttestationDomain
    message: AttestationMessage
    version: int
    signature: AttestationSignatureFields
    primaryType: str


class AttestationPayload(BaseModel):
    sig: AttestationSig
    signer: str

    # Pydantic v2 validation hook
    @field_validator("sig")
    @classmethod
    def check_sig(cls, v: AttestationSig):
        # basic structural sanity without crypto:
        # - uid hex
        hex_to_bytes(v.uid)
        # - recipient hex
        hex_to_bytes(v.message.recipient)
        # - chainId int'able
        int(v.domain.chainId)
        # - time is unix-like
        unix_str_to_datetime(v.message.time)
        return v


class BulkAttestationRequest(BaseModel):
    attestations: List[AttestationPayload] = Field(..., min_items=1, max_items=1000)


class BulkAttestationResponse(BaseModel):
    accepted: int
    duplicates: int
    failed_validation: List[Dict[str, Any]]
    status: str = "queued"


class SingleAttestationResponse(BaseModel):
    uid: str
    status: str = "queued"

class LabelItem(BaseModel):
    tag_id: str
    tag_value: str
    chain_id: str
    time: datetime
    attester: Optional[str]

class LabelsResponse(BaseModel):
    address: str
    count: int
    labels: List[LabelItem]
    
class BulkLabelsRequest(BaseModel):
    addresses: List[str] = Field(..., min_items=1, max_items=100)
    chain_id: Optional[str] = Field(
        None,
        description="Optional chain_id filter, e.g. 'eip155:8453'"
    )
    limit_per_address: int = Field(
        50,
        ge=1,
        le=1000,
        description="Max labels to return per address in the response"
    )
    include_all: bool = False

class AddressLabels(BaseModel):
    address: str
    labels: List[LabelItem]

class BulkLabelsResponse(BaseModel):
    results: List[AddressLabels]
    
class AddressWithLabel(BaseModel):
    address: str
    chain_id: str
    time: datetime
    attester: Optional[str]


class LabelSearchResponse(BaseModel):
    tag_id: str
    tag_value: Optional[str]
    count: int
    results: List[AddressWithLabel]
    
class AttestationRecord(BaseModel):
    uid: str
    time: datetime
    chain_id: Optional[str]
    attester: str
    recipient: Optional[str]  # <- was str
    revoked: bool
    is_offchain: bool
    ipfs_hash: Optional[str]
    schema_info: str
    tags_json: Optional[Dict[str, Any]]
    #raw: Optional[Dict[str, Any]]

class AttestationQueryResponse(BaseModel):
    count: int
    attestations: List[AttestationRecord]
    
    
class AttesterAnalytics(BaseModel):
    attester: str
    unique_attestations: int


class AttesterAnalyticsResponse(BaseModel):
    count: int
    results: List[AttesterAnalytics]
    
class TrustListRecord(BaseModel):
    uid: str
    time: datetime
    attester: Optional[str]
    recipient: Optional[str]
    revoked: bool
    is_offchain: bool
    ipfs_hash: Optional[str]
    schema_info: Optional[str]
    owner_name: Optional[str]
    attesters: Optional[Any]
    attestations: Optional[Any]

class TrustListQueryResponse(BaseModel):
    count: int
    trust_lists: List[TrustListRecord]

class TrustListPostResponse(BaseModel):
    uid: str
    status: str = "queued"
    
#
# CRYPTO / VERIFICATION
#

def parse_caip10(caip10: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(caip10, str):
        return (None, None)
    if caip10.count(":") != 2:
        return (None, None)
    namespace, reference, account = caip10.split(":", 2)
    if not namespace or not reference or not account:
        return (None, None)
    return (f"{namespace}:{reference}", account)


def decode_attestation_data(msg: AttestationMessage) -> Tuple[Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    """
    Decode msg.data as (string subject, string tags_json), supporting:
      - new schema: subject == CAIP10 (namespace:reference:account)
      - old schema: subject == chain_id (namespace:reference)

    Returns:
      (chain_id_str, recipient_str, tags_dict)
    where recipient_str is only set for CAIP10 inputs.
    """

    if not isinstance(msg.data, str):
        return (None, None, None)
    if not msg.data.startswith("0x"):
        return (None, None, None)

    # strip "0x", decode hex -> bytes
    raw_bytes = hex_to_bytes(msg.data)

    try:
        decoded_tuple = abi_decode(
            ["string", "string"],
            raw_bytes,
        )
    except Exception:
        # doesn't match expected tuple layout
        return (None, None, None)

    if not isinstance(decoded_tuple, (list, tuple)) or len(decoded_tuple) != 2:
        return (None, None, None)

    subject_str = decoded_tuple[0]
    tags_json_str = decoded_tuple[1]

    chain_id_str = None
    recipient_str = None
    if isinstance(subject_str, str):
        colon_count = subject_str.count(":")
        if colon_count == 2:
            chain_id_str, recipient_str = parse_caip10(subject_str)
            if not chain_id_str or not recipient_str:
                return (None, None, None)
        elif colon_count == 1:
            chain_id_str = subject_str
        else:
            return (None, None, None)
    else:
        return (None, None, None)

    # tags_json_str should itself be valid JSON
    try:
        tags_dict = json.loads(tags_json_str)
    except Exception:
        tags_dict = None

    return (chain_id_str, recipient_str, tags_dict)


def left_pad_to_32(b: bytes) -> bytes:
    if len(b) > 32:
        # should never happen in a valid sig, but let's be loud if it does
        raise ValueError("signature component longer than 32 bytes")
    if len(b) < 32:
        return b"\x00" * (32 - len(b)) + b
    return b

def recover_signer_address(payload: AttestationPayload) -> str:
    sig = payload.sig
    msg = sig.message
    domain = sig.domain

    typed_data = {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            sig.primaryType: [
                {"name": f.name, "type": f.type} for f in sig.types.Attest
            ],
        },
        "domain": {
            "name": domain.name,
            "version": domain.version,
            "chainId": int(domain.chainId),
            "verifyingContract": domain.verifyingContract,
        },
        "primaryType": sig.primaryType,
        "message": {
            "version": msg.version,
            "schema": msg.schema_id,
            "recipient": msg.recipient,
            "time": int(msg.time),
            "expirationTime": int(msg.expirationTime),
            "revocable": msg.revocable,
            "refUID": msg.refUID,
            "data": msg.data,
            "salt": msg.salt,
        },
    }

    # build EIP-712 signable message from our typed data
    encoded = encode_typed_data(full_message=typed_data)

    # decode r, s, pad to 32 bytes
    r_raw = hex_to_bytes(sig.signature.r)
    s_raw = hex_to_bytes(sig.signature.s)

    r_bytes = left_pad_to_32(r_raw)
    s_bytes = left_pad_to_32(s_raw)

    # normalize v
    v_int = sig.signature.v
    # some wallets output 27/28, some output 0/1.
    # eth-account can handle 27/28 fine with recover_message.
    # but to be safe, map 0/1 -> 27/28.
    if v_int in (0, 1):
        v_int = v_int + 27

    v_byte = bytes([v_int])

    full_sig = r_bytes + s_bytes + v_byte  # should now always be 65 bytes

    # try recovering using recover_message, fallback to recover_hash
    try:
        recovered = Account.recover_message(encoded, signature=full_sig)
    except Exception:
        hash_bytes = None
        if hasattr(encoded, "hash_structured_data"):
            hash_bytes = encoded.hash_structured_data()
        elif hasattr(encoded, "hash"):
            hash_bytes = encoded.hash

        if hash_bytes is None:
            raise

        recovered = Account.recover_hash(hash_bytes, signature=full_sig)

    return to_normalized_address(recovered)


def verify_attestation(payload: AttestationPayload) -> None:
    if payload.sig.primaryType != "Attest":
        raise HTTPException(status_code=400, detail="Expected primaryType=Attest")
    recovered_addr = recover_signer_address(payload)
    if recovered_addr != to_normalized_address(payload.signer):
        raise HTTPException(status_code=400, detail="Signature invalid or signer mismatch")
    
    claimed_addr = to_normalized_address(payload.signer)
    if recovered_addr != claimed_addr:
        raise HTTPException(status_code=400, detail="Signature invalid or signer mismatch")
    
# ---- TRUST LIST via EAS schema support ----

TRUST_LIST_SCHEMA = "0x6d780a85bfad501090cd82868a0c773c09beafda609d54888a65c106898c363d"

def decode_trust_list_data(msg: AttestationMessage) -> Tuple[Optional[str], Optional[Any], Optional[Any]]:
    """
    For schema TRUST_LIST_SCHEMA, message.data is expected to decode as:
        (string owner_name, string attesters_json, string attestations_json)
    Returns: (owner_name, attesters_obj, attestations_obj)
    """
    if not isinstance(msg.data, str) or not msg.data.startswith("0x"):
        return (None, None, None)

    raw_bytes = hex_to_bytes(msg.data)
    try:
        # adjust types to match your actual ABI encoder
        owner_name, attesters_json, attestations_json = abi_decode(
            ['string', 'string', 'string'],
            raw_bytes
        )
    except Exception:
        return (None, None, None)

    def _parse(s):
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return None

    return (owner_name, _parse(attesters_json), _parse(attestations_json))

#
# LIFESPAN (replaces @app.on_event start/stop)
#

@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=10)
    app.state.db = pool
    try:
        yield
    finally:
        process_pool.shutdown(wait=True)
        await pool.close()


app = FastAPI(
    title="Open Labels Initiative: Label Pool API",
    lifespan=lifespan,
)

# from fastapi.middleware.cors import CORSMiddleware
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["https://www.growthepie.com"],  # or env-driven
#     allow_methods=["GET","POST","OPTIONS"],
#     allow_headers=["*"],
#     expose_headers=[],
# )


#
# DB INSERT
#

async def insert_attestations(conn, rows: List[dict]):
    if not rows:
        return 0, 0

    cols = [
        "uid",
        "time",
        "chain_id",
        "attester",
        "recipient",
        "revoked",
        "is_offchain",
        "tx_hash",
        "ipfs_hash",
        "revocation_time",
        "tags_json",
        "raw",
        "schema_info",
        "last_updated_time"
    ]

    values_sql_parts = []
    params = []
    param_i = 1

    for r in rows:
        placeholders = []
        for c in cols:
            placeholders.append(f"${param_i}")
            params.append(r[c])
            param_i += 1
        values_sql_parts.append("(" + ",".join(placeholders) + ")")

    sql = f"""
        INSERT INTO public.attestations
        ({", ".join(cols)})
        VALUES {", ".join(values_sql_parts)}
        ON CONFLICT (uid) DO NOTHING
        RETURNING uid;
    """

    inserted_uids = await conn.fetch(sql, *params)
    inserted_count = len(inserted_uids)
    duplicate_count = len(rows) - inserted_count

    return inserted_count, duplicate_count

async def insert_trust_list(conn, row: dict) -> bool:
    sql = """
        INSERT INTO public.trust_lists (
            uid, "time", attester, recipient, revoked, is_offchain,
            tx_hash, ipfs_hash, revocation_time, raw, last_updated_time,
            schema_info, owner_name, attesters, attestations
        ) VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9, $10::jsonb, $11,
            $12, $13, $14::jsonb, $15::jsonb
        )
        ON CONFLICT (uid) DO NOTHING
        RETURNING uid;
    """
    r = await conn.fetchrow(sql,
        row["uid"], row["time"], row["attester"], row["recipient"], row["revoked"], row["is_offchain"],
        row["tx_hash"], row["ipfs_hash"], row["revocation_time"], row["raw"], row["last_updated_time"],
        row["schema_info"], row["owner_name"], row["attesters"], row["attestations"]
    )
    return r is not None


def build_row_from_payload(att: AttestationPayload):
    sig = att.sig
    msg = sig.message
    domain = sig.domain

    uid_bytes = hex_to_bytes(sig.uid)
    attester_bytes = hex_to_bytes(att.signer)
    ts = unix_str_to_datetime(msg.time)

    # decode structured data from msg.data
    decoded_chain_id, decoded_recipient, decoded_tags_dict = decode_attestation_data(msg)
    if not decoded_chain_id:
        raise HTTPException(
            status_code=400,
            detail="Invalid attestation data; expected chain_id or CAIP10",
        )
    recipient_value = decoded_recipient if decoded_recipient is not None else msg.recipient
    try:
        recipient_bytes = hex_to_bytes(recipient_value)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid recipient address")

    # Prepare tags_json for DB
    if decoded_tags_dict is not None:
        tags_json_db_value = json.dumps(decoded_tags_dict)
    else:
        tags_json_db_value = None

    row = {
        "uid": uid_bytes,
        "time": ts,
        "chain_id": decoded_chain_id, 
        "attester": attester_bytes,
        "recipient": recipient_bytes,
        "revoked": False,
        "is_offchain": True,
        "tx_hash": None,
        "ipfs_hash": None,
        "revocation_time": None,
        "tags_json": tags_json_db_value,                      # <-- now filled
        "raw": json.dumps(att.model_dump(by_alias=True)),     # full original payload
        "schema_info": f"{domain.chainId}__{msg.schema_id}",
        "last_updated_time": datetime.now(timezone.utc),
        "source": "oli_api"
    }

    return row

def _parse_optional_ts(v: Optional[str]) -> Optional[datetime]:
    if not v:
        return None
    try:
        return datetime.fromtimestamp(float(v), tz=timezone.utc)
    except ValueError:
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

def build_trust_list_row_from_eas(att: AttestationPayload) -> dict:
    sig = att.sig
    msg = sig.message
    domain = sig.domain

    # sanity: ensure schema matches our trust-list schema
    if msg.schema_id.lower() != TRUST_LIST_SCHEMA:
        raise HTTPException(status_code=400, detail="Wrong schema for trust list payload")

    owner_name, attesters_obj, attestations_obj = decode_trust_list_data(msg)
    if owner_name is None and attesters_obj is None and attestations_obj is None:
        raise HTTPException(status_code=400, detail="Trust list data could not be decoded")

    # signer == attester policy (optional but recommended)
    signer_norm = to_normalized_address(att.signer)
    # EAS Attest does not carry 'attester' in the message; attester is the signer.
    attester_bytes = hex_to_bytes(signer_norm)

    # uid / recipient / tx fields
    uid_bytes       = hex_to_bytes(sig.uid)
    recipient_bytes = hex_to_bytes(msg.recipient) if msg.recipient else None
    tx_hash_bytes   = None  # offchain (set if you later include onchain TLs)

    # times
    time_dt = unix_str_to_datetime(msg.time)
    rev_dt  = None

    return {
        "uid": uid_bytes,
        "time": time_dt,
        "attester": attester_bytes,
        "recipient": recipient_bytes,
        "revoked": False,
        "is_offchain": True,                      # TL via EAS offchain
        "tx_hash": tx_hash_bytes,
        "ipfs_hash": None,                        # fill if you pin
        "revocation_time": rev_dt,
        "raw": json.dumps(att.model_dump(by_alias=True)),
        "last_updated_time": datetime.now(timezone.utc),
        "schema_info": f"{domain.chainId}__{msg.schema_id}",
        "owner_name": owner_name,
        "attesters": json.dumps(attesters_obj) if attesters_obj is not None else None,
        "attestations": json.dumps(attestations_obj) if attestations_obj is not None else None,
    }
def _row_to_trust_list_record(r) -> TrustListRecord:
    uid_hex       = _bytes_to_hexmaybe(r["uid"])
    attester_hex  = _bytes_to_hexmaybe(r["attester"]) if r["attester"] is not None else None
    recipient_hex = _bytes_to_hexmaybe(r["recipient"]) if r["recipient"] is not None else None

    # JSONB decoding (in case driver returns text)
    def _jsonify(v):
        if v is None: return None
        if isinstance(v, (dict, list)): return v
        try:
            return json.loads(v)
        except Exception:
            return None

    return TrustListRecord(
        uid=uid_hex or "",
        time=r["time"],
        attester=attester_hex,
        recipient=recipient_hex,
        revoked=r["revoked"],
        is_offchain=r["is_offchain"],
        ipfs_hash=r["ipfs_hash"],
        schema_info=r["schema_info"],
        owner_name=r["owner_name"],
        attesters=_jsonify(r["attesters"]),
        attestations=_jsonify(r["attestations"]),
    )

#
# USAGE LOGGING
#

MAX_BODY_BYTES = 10 * 1024 * 1024  # 10MB

@app.middleware("http")
async def max_body_guard(request: Request, call_next):
    cl = request.headers.get("content-length")
    if cl and cl.isdigit() and int(cl) > MAX_BODY_BYTES:
        return JSONResponse({"detail": "Payload too large"}, status_code=413)
    return await call_next(request)

@app.middleware("http")
async def usage_logger(request: Request, call_next):
    response = await call_next(request)

    key_id = getattr(request.state, "api_key_id", None)
    # Case A: authenticated route (dependency ran) -> we already know the key_id
    if key_id:
        async with app.state.db.acquire() as conn:
            await conn.execute("""
                UPDATE public.api_keys
                   SET usage_count = usage_count + 1, last_used_at = now()
                 WHERE id = $1
            """, key_id)
            await conn.execute("""
                INSERT INTO public.api_key_usage(key_id, endpoint, status_code, ip)
                VALUES ($1, $2, $3, $4::inet)
            """, key_id, request.url.path, response.status_code,
               (request.client.host if request.client else None))
        return response

    # Case B (optional): public route — best-effort log if a plausible key header is present
    api_key = request.headers.get(API_KEY_HEADER)
    if api_key:
        parsed = hash_presented_key(api_key)
        if parsed:
            prefix, _ = parsed
            # IMPORTANT: we are not authenticating here, just trying to attribute usage.
            # Use the L1 cache-backed lookup to avoid extra DB reads when warm.
            async with app.state.db.acquire() as conn:
                row = await _lookup_key_by_prefix(conn, prefix)  # uses LRU TTL cache internally
                if row and not row.get("revoked_at"):
                    await conn.execute("""
                        UPDATE public.api_keys
                           SET usage_count = usage_count + 1, last_used_at = now()
                         WHERE id = $1
                    """, row["id"])
                    await conn.execute("""
                        INSERT INTO public.api_key_usage(key_id, endpoint, status_code, ip)
                        VALUES ($1, $2, $3, $4::inet)
                    """, row["id"], request.url.path, response.status_code,
                       (request.client.host if request.client else None))

    return response

#
# ROUTES
#

## Attestation endpoints

@app.post("/attestation", response_model=SingleAttestationResponse, tags=["Attestation: Labels"])
async def post_single_attestation(payload: AttestationPayload):
    # Full cryptographic verification
    verify_attestation(payload)

    row = build_row_from_payload(payload)

    async with app.state.db.acquire() as conn:
        async with conn.transaction():
            inserted, dupes = await insert_attestations(conn, [row])
            print(f"Inserted: {inserted}, Dupes: {dupes}")

    return SingleAttestationResponse(
        uid=payload.sig.uid,
        status="queued"
    )


def verify_and_build_sync(att_dict: dict) -> dict:
    # Recreate the object from plain data
    payload = AttestationPayload.model_validate(att_dict)

    # This will raise HTTPException(400) on bad sig
    verify_attestation(payload)

    # Build the row (no async inside here, just pure compute)
    row = build_row_from_payload(payload)

    return row

@app.post("/attestations/bulk", response_model=BulkAttestationResponse, tags=["Attestation: Labels"])
async def post_bulk_attestations(req: BulkAttestationRequest):
    t0 = time.perf_counter()

    # 1. schedule verify+build for each attestation in parallel across processes
    t_verify_start = time.perf_counter()

    loop = asyncio.get_running_loop()
    tasks = []
    for att in req.attestations:
        att_dict = att.model_dump(by_alias=True)  # plain dict for pickling
        tasks.append(loop.run_in_executor(process_pool, verify_and_build_sync, att_dict))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    t_verify_end = time.perf_counter()

    valid_rows = []
    failed_validation: List[Dict[str, Any]] = []

    for idx, result in enumerate(results):
        if isinstance(result, Exception):
            # If verify_attestation raised HTTPException, keep its reason
            if isinstance(result, HTTPException):
                failed_validation.append({
                    "index": idx,
                    "reason": result.detail,
                })
            else:
                failed_validation.append({
                    "index": idx,
                    "reason": str(result),
                })
        else:
            valid_rows.append(result)

    # 2. insert
    t_insert_start = time.perf_counter()
    inserted = 0
    dupes = 0
    if valid_rows:
        async with app.state.db.acquire() as conn:
            async with conn.transaction():
                inserted, dupes = await insert_attestations(conn, valid_rows)
    t_insert_end = time.perf_counter()

    t_done = time.perf_counter()

    print({
        "count": len(req.attestations),
        "verify_sec": t_verify_end - t_verify_start,
        "insert_sec": t_insert_end - t_insert_start,
        "total_sec": t_done - t0,
        "accepted_after_verify": len(valid_rows),
        "failed_validation": len(failed_validation),
    })

    return BulkAttestationResponse(
        accepted=inserted,
        duplicates=dupes,
        failed_validation=failed_validation,
        status="queued"
    )
    
@app.get(
    "/attestations", 
    response_model=AttestationQueryResponse,
    tags=["Attestation: Labels"]
    )
async def get_attestations(
    uid: Optional[str] = Query(
        None, description="Filter by specific attestation UID (0x...)"
    ),
    chain_id: Optional[str] = Query(
        None, description="Filter by chain_id (e.g. 'eip155:8453')"
    ),
    attester: Optional[str] = Query(
        None, description="Filter by attester address (0x...)"
    ),
    recipient: Optional[str] = Query(
        None, description="Filter by recipient address (0x...)"
    ),
    schema_info: Optional[str] = Query(
        None, description="Filter by schema_info (e.g. '8453__0xabc...')"
    ),
    since: Optional[str] = Query(
        None,
        description="Return only attestations created after this timestamp (ISO8601 or Unix seconds)"
    ),
    order: Literal["asc", "desc"] = Query(
        "desc",
        description="Order results by attestation time (asc or desc). Default: desc"
    ),
    limit: int = Query(
        30, ge=1, le=1000, description="Max number of attestations to return"
    ),
):
    """
    Return raw attestations from storage.

    Behavior:
    - If `uid` is provided: return only that attestation (ignore other filters, limit=1).
    - Else: filter by any combination of {attester, recipient, schema_info, since}.
    - Results ordered by time (desc by default).
    """

    async with app.state.db.acquire() as conn:
        # ---- Case 1: UID lookup ----
        if uid:
            uid_bytes = hex_to_bytes(uid)
            row = await conn.fetchrow(
                """
                SELECT
                    uid,
                    time,
                    chain_id,
                    attester,
                    recipient,
                    revoked,
                    is_offchain,
                    ipfs_hash,
                    schema_info,
                    tags_json
                FROM public.attestations
                WHERE uid = $1
                LIMIT 1;
                """,
                uid_bytes,
            )
            if row is None:
                return AttestationQueryResponse(count=0, attestations=[])
            rec = _row_to_attestation_record(row)
            return AttestationQueryResponse(count=1, attestations=[rec])

        # ---- Case 2: Dynamic filters ----
        where_clauses = []
        params = []
        idx = 1
        
        if chain_id:
            where_clauses.append(f"chain_id = ${idx}")
            params.append(chain_id)
            idx += 1

        if attester:
            where_clauses.append(f"attester = ${idx}")
            params.append(hex_to_bytes(attester.lower()))
            idx += 1

        if recipient:
            where_clauses.append(f"recipient = ${idx}")
            params.append(hex_to_bytes(recipient.lower()))
            idx += 1

        if schema_info:
            where_clauses.append(f"schema_info = ${idx}")
            params.append(schema_info)
            idx += 1

        # Parse optional `since`
        if since:
            try:
                ts = float(since)
                since_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            except ValueError:
                since_dt = datetime.fromisoformat(since)
                if since_dt.tzinfo is None:
                    since_dt = since_dt.replace(tzinfo=timezone.utc)

            where_clauses.append(f"time > ${idx}")
            params.append(since_dt)
            idx += 1

        # Join WHERE clauses
        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        # Enforce safe ordering (avoid SQL injection)
        order = order.lower()
        order_sql = "DESC" if order == "desc" else "ASC"

        sql = f"""
            SELECT
                uid,
                time,
                chain_id,
                attester,
                recipient,
                revoked,
                is_offchain,
                ipfs_hash,
                schema_info,
                tags_json
            FROM public.attestations
            {where_sql}
            ORDER BY time {order_sql}
            LIMIT ${idx};
        """

        params.append(int(limit))
        rows = await conn.fetch(sql, *params)

    attestations_out = [_row_to_attestation_record(r) for r in rows]

    return AttestationQueryResponse(
        count=len(attestations_out),
        attestations=attestations_out,
    )
    
## Trust List endpoints
@app.post("/trust-list", response_model=TrustListPostResponse, tags=["Attestation: Trust Lists"])
async def post_trust_list(payload: AttestationPayload):
    """
    Accept a Trust List attestation encoded as a standard EAS Attest typed-data,
    signed by the trust-list issuer.
    """
    # 1) Verify EIP-712 signature (already battle-tested for /attestation)
    verify_attestation(payload)

    # 2) Build DB row from the EAS message for the TL schema
    row = build_trust_list_row_from_eas(payload)

    # 3) Insert
    async with app.state.db.acquire() as conn:
        async with conn.transaction():
            await insert_trust_list(conn, row)

    return TrustListPostResponse(uid=payload.sig.uid, status="queued")


@app.get("/trust-lists", response_model=TrustListQueryResponse, tags=["Attestation: Trust Lists"])
async def get_trust_lists(
    uid: Optional[str] = Query(None, description="Filter by specific trust list UID (0x...)"),
    attester: Optional[str] = Query(None, description="Filter by attester address (0x...)"),
    order: Literal["asc","desc"] = Query("desc", description="Order by time (default: desc)"),
    limit: int = Query(10, ge=1, le=1000, description="Max number of rows to return (default 10)"),
):
    async with app.state.db.acquire() as conn:
        # Case 1: direct UID lookup
        if uid:
            row = await conn.fetchrow(
                """
                SELECT uid, "time", attester, recipient, revoked, is_offchain,
                       tx_hash, ipfs_hash, revocation_time, raw, last_updated_time,
                       schema_info, owner_name, attesters, attestations
                  FROM public.trust_lists
                 WHERE uid = $1
                 LIMIT 1;
                """,
                hex_to_bytes(uid),
            )
            if not row:
                return TrustListQueryResponse(count=0, trust_lists=[])
            return TrustListQueryResponse(count=1, trust_lists=[_row_to_trust_list_record(row)])

        # Case 2: filtered listing (latest by default)
        where, params = [], []
        i = 1
        if attester:
            where.append(f"attester = ${i}")
            params.append(hex_to_bytes(attester.lower()))
            i += 1

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        order_sql = "DESC" if order.lower() == "desc" else "ASC"

        sql = f"""
            SELECT uid, "time", attester, recipient, revoked, is_offchain,
                   tx_hash, ipfs_hash, revocation_time, raw, last_updated_time,
                   schema_info, owner_name, attesters, attestations
              FROM public.trust_lists
              {where_sql}
             ORDER BY "time" {order_sql}
             LIMIT ${i};
        """
        params.append(int(limit))
        rows = await conn.fetch(sql, *params)

    out = [_row_to_trust_list_record(r) for r in rows]
    return TrustListQueryResponse(count=len(out), trust_lists=out)


# @app.get(
#     "/compute_trust_table",
#     dependencies=[Depends(get_api_key)],
#     tags=["Attestation: Trust Lists"],
# )
# async def compute_trust_table(
#     source_node: str = Query(
#         ..., description="Ethereum address (0x...) used as the trust graph source node"
#     ),
# ):
#     """Compute and return a trust table for the provided source node."""
#     try:
#         normalized_source = to_normalized_address(source_node)
#     except Exception:
#         raise HTTPException(status_code=400, detail="Invalid source_node address")


#     try:
#         oli.trust = UtilsTrust(oli)
#         trust_table = oli.trust.compute_trust_table(
#             oli.trust.TrustGraph,
#             source_node=normalized_source,
#         )
#     except Exception as exc:
#         raise HTTPException(
#             status_code=500,
#             detail=f"Failed to compute trust table: {exc}",
#         ) from exc

#     if not isinstance(trust_table, dict):
#         try:
#             trust_table = dict(trust_table)
#         except Exception:
#             raise HTTPException(
#                 status_code=500,
#                 detail="Trust table result could not be serialized",
#             )

#     return {"source_node": normalized_source, "trust_table": trust_table}


### Labels endpoints

def normalize_eth_address(addr: str) -> str:
    a = addr.strip().lower()
    if not a.startswith("0x"):
        a = "0x" + a
    return a

def _bytes_to_hexmaybe(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        return "0x" + v.hex()
    # assume it's already string-ish
    s = str(v)
    # normalize to lower 0x... if it looks like hex without 0x
    if s.startswith("0x") or s.startswith("0X"):
        return s.lower()
    # if it's 64-char hex without 0x, add it
    if all(c in "0123456789abcdefABCDEF" for c in s) and len(s) in (40, 64):
        return "0x" + s.lower()
    return s.lower()

def _row_to_attestation_record(r) -> AttestationRecord:
    uid_hex = _bytes_to_hexmaybe(r["uid"])
    attester_hex = _bytes_to_hexmaybe(r["attester"])
    recipient_hex = _bytes_to_hexmaybe(r["recipient"])

    # tags_json handling:
    tags_val = r["tags_json"]
    # If it's a string of JSON, parse it
    if isinstance(tags_val, str):
        try:
            tags_val = json.loads(tags_val)
        except Exception:
            # fallback: leave as None if it failed
            tags_val = None
    # If it's None or already dict, fine.

    return AttestationRecord(
        uid=uid_hex or "",
        time=r["time"],
        chain_id=r["chain_id"],
        attester=attester_hex,
        recipient=recipient_hex,
        revoked=r["revoked"],
        is_offchain=r["is_offchain"],
        ipfs_hash=r["ipfs_hash"],
        schema_info=r["schema_info"],
        tags_json=tags_val,
        #raw=raw_val,
    )

@app.get(
    "/labels",
    response_model=LabelsResponse,
    dependencies=[Depends(get_api_key)],
    tags=["Labels"],
)
async def get_labels(
    address: str = Query(..., description="Address (0x...)"),
    chain_id: Optional[str] = Query(None, description="Optional chain_id filter"),
    limit: int = Query(100, le=1000, description="Max number of labels to return"),
    include_all: bool = Query(False, description="If false (default), return only the latest label per (chain_id, attester, tag_id)"),
):
    """Return labels (key/value) for a given address. By default, only the newest per (chain_id, attester, tag_id)."""

    # normalize address
    addr_norm = address.lower()
    if not addr_norm.startswith("0x"):
        addr_norm = "0x" + addr_norm

    where_clauses = ["address = $1"]
    params = [hex_to_bytes(addr_norm)]
    next_param = 2

    if chain_id:
        where_clauses.append(f"chain_id = ${next_param}")
        params.append(chain_id)
        next_param += 1

    # LIMIT is always the last param
    limit_param_num = next_param
    params.append(int(limit))

    if include_all:
        # Return all label rows (no collapsing)
        sql = f"""
            SELECT
                chain_id,
                tag_id,
                tag_value,
                time,
                attester
            FROM public.labels
            WHERE {' AND '.join(where_clauses)}
            ORDER BY time DESC
            LIMIT ${limit_param_num};
        """
    else:
        # Return only latest per (chain_id, attester, tag_id)
        # DISTINCT ON requires ORDER BY keys first, then time DESC
        sql = f"""
            SELECT DISTINCT ON (chain_id, attester, tag_id)
                chain_id,
                tag_id,
                tag_value,
                time,
                attester
            FROM public.labels
            WHERE {' AND '.join(where_clauses)}
            ORDER BY chain_id, attester, tag_id, time DESC
            LIMIT ${limit_param_num};
        """

    async with app.state.db.acquire() as conn:
        await conn.execute("SET LOCAL statement_timeout = '5s'")
        rows = await conn.fetch(sql, *params)

    labels = []
    for r in rows:
        attester_val = r["attester"]
        if isinstance(attester_val, (bytes, bytearray)):
            attester_hex = "0x" + attester_val.hex()
        else:
            attester_hex = attester_val

        labels.append(
            LabelItem(
                tag_id=r["tag_id"],
                tag_value=r["tag_value"],
                chain_id=r["chain_id"],
                time=r["time"],
                attester=attester_hex,
            )
        )

    return LabelsResponse(
        address=addr_norm,
        count=len(labels),
        labels=labels,
    )
    
@app.post(
    "/labels/bulk", 
    response_model=BulkLabelsResponse, 
    dependencies=[Depends(get_api_key)],
    tags=["Labels"]
    )
async def get_labels_bulk(req: BulkLabelsRequest):
    # 1. normalize and dedupe input addresses
    normalized_addrs = [normalize_eth_address(a) for a in req.addresses]
    orig_to_norm = {orig: normalize_eth_address(orig) for orig in req.addresses}

    # convert to bytes for DB match
    addr_bytes_list = [hex_to_bytes(a) for a in normalized_addrs]

    unique_addr_bytes_list = []
    seen = set()
    for b in addr_bytes_list:
        if b not in seen:
            seen.add(b)
            unique_addr_bytes_list.append(b)

    # 2. build SQL dynamically based on optional chain_id and include_all
    params = [unique_addr_bytes_list]
    chain_filter_sql = ""

    if req.chain_id:
        chain_filter_sql = "AND l.chain_id = $2"
        params.append(req.chain_id)

    if req.include_all:
        # No collapse: return all labels then enforce per-address limit in Python
        sql = f"""
            SELECT
                l.address,
                l.chain_id,
                l.tag_id,
                l.tag_value,
                l."time",
                l.attester
            FROM public.labels AS l
            WHERE l.address = ANY($1)
            {chain_filter_sql}
            ORDER BY l."time" DESC;
        """
    else:
        # Collapse to latest per (address, chain_id, attester, tag_id)
        # DISTINCT ON: order by those keys first, then time desc
        sql = f"""
            SELECT DISTINCT ON (l.address, l.chain_id, l.attester, l.tag_id)
                l.address,
                l.chain_id,
                l.tag_id,
                l.tag_value,
                l."time",
                l.attester
            FROM public.labels AS l
            WHERE l.address = ANY($1)
            {chain_filter_sql}
            ORDER BY l.address, l.chain_id, l.attester, l.tag_id, l."time" DESC;
        """

    async with app.state.db.acquire() as conn:
        rows = await conn.fetch(sql, *params)

    # 3. group rows by address
    grouped: Dict[str, List[LabelItem]] = {}

    for r in rows:
        addr_bytes = r["address"]
        if isinstance(addr_bytes, (bytes, bytearray)):
            addr_hex = "0x" + addr_bytes.hex()
        else:
            addr_hex = str(addr_bytes).lower()

        attester_val = r["attester"]
        if isinstance(attester_val, (bytes, bytearray)):
            attester_hex = "0x" + attester_val.hex()
        else:
            attester_hex = attester_val

        item = LabelItem(
            tag_id=r["tag_id"],
            tag_value=r["tag_value"],
            chain_id=r["chain_id"],
            time=r["time"],
            attester=attester_hex,
        )

        grouped.setdefault(addr_hex, []).append(item)

    # 4. enforce per-address limit_per_address and keep original order
    results: List[AddressLabels] = []
    for orig in req.addresses:
        norm = orig_to_norm[orig]
        labels_for_addr = grouped.get(norm, [])
        limited = labels_for_addr[: req.limit_per_address]
        results.append(AddressLabels(address=norm, labels=limited))

    return BulkLabelsResponse(results=results)

@app.get(
    "/addresses/search", 
    response_model=LabelSearchResponse, 
    dependencies=[Depends(get_api_key)],
    tags=["Labels"]
    )
async def search_addresses_by_tag(
    tag_id: str = Query(..., description="The tag key, e.g. 'usage_category'"),
    tag_value: Optional[str] = Query(None, description="Optional tag value, e.g. 'dex'"),
    chain_id: Optional[str] = Query(None, description="Optional chain_id filter, e.g. 'eip155:8453'"),
    limit: int = Query(100, ge=1, le=1000, description="Max number of addresses to return"),
):
    """
    Return all addresses that have a specific tag_id and optional tag_value.
    """

    # Build WHERE dynamically
    where_clauses = [
        "l.tag_id = $1",
    ]
    params = [tag_id]
    next_param = 2

    if tag_value is not None:
        where_clauses.append(f"l.tag_value = ${next_param}")
        params.append(tag_value)
        next_param += 1

    if chain_id:
        where_clauses.append(f"l.chain_id = ${next_param}")
        params.append(chain_id)
        next_param += 1

    # LIMIT is always the last parameter
    limit_param_num = next_param
    params.append(int(limit))

    sql = f"""
        SELECT
            l.address,
            l.chain_id,
            l.time,
            l.attester
        FROM public.labels AS l
        WHERE {' AND '.join(where_clauses)}
        ORDER BY l.time DESC
        LIMIT ${limit_param_num};
    """

    async with app.state.db.acquire() as conn:
        await conn.execute("SET LOCAL statement_timeout = '5s'")
        rows = await conn.fetch(sql, *params)

    results: List[AddressWithLabel] = []

    for r in rows:
        # normalize address bytes → 0x...
        addr_bytes = r["address"]
        if isinstance(addr_bytes, (bytes, bytearray)):
            addr_hex = "0x" + addr_bytes.hex()
        else:
            addr_hex = str(addr_bytes).lower()

        # normalize attester bytes → 0x...
        attester_val = r["attester"]
        if isinstance(attester_val, (bytes, bytearray)):
            attester_hex = "0x" + attester_val.hex()
        else:
            attester_hex = attester_val

        results.append(
            AddressWithLabel(
                address=addr_hex,
                chain_id=r["chain_id"],
                time=r["time"],
                attester=attester_hex,
            )
        )

    return LabelSearchResponse(
        tag_id=tag_id,
        tag_value=tag_value,
        count=len(results),
        results=results,
    )
    
@app.get(
    "/analytics/attesters", 
    response_model=AttesterAnalyticsResponse, 
    dependencies=[Depends(get_api_key)],
    tags=["Analytics"]
    )
async def get_attester_analytics(
    chain_id: Optional[str] = Query(
        None, description="Optional chain_id filter, e.g. 'eip155:8453'"
    ),
    limit: int = Query(20, ge=1, le=100, description="Number of rows to return"),
):
    """
    Analytics summary: group by attester, count number of labels and unique attestations.
    """

    chain_filter = ""
    params = [int(limit)]

    if chain_id:
        chain_filter = "AND chain_id = $1"
        params = [chain_id, int(limit)]

    sql = f"""
        SELECT
            attester,
            COUNT(*) AS unique_attestations
        FROM public.attestations
        WHERE TRUE
        {chain_filter}
        GROUP BY attester
        ORDER BY unique_attestations DESC
        LIMIT ${len(params)};
    """

    async with app.state.db.acquire() as conn:
        await conn.execute("SET LOCAL statement_timeout = '5s'")
        rows = await conn.fetch(sql, *params)

    results = []
    for r in rows:
        attester_val = r["attester"]
        if isinstance(attester_val, (bytes, bytearray)):
            attester_hex = "0x" + attester_val.hex()
        else:
            attester_hex = str(attester_val)
        results.append(
            AttesterAnalytics(
                attester=attester_hex,
                unique_attestations=r["unique_attestations"],
            )
        )

    return AttesterAnalyticsResponse(
        count=len(results),
        results=results,
    )
    
## ADMIN

class CreateKeyRequest(BaseModel):
    owner_id: str
    metadata: Optional[Dict] = None

class CreateKeyResponse(BaseModel):
    api_key: str      # show once
    id: str

def require_admin(authz: Optional[str] = Header(None, alias="Authorization")):
    if not authz or not authz.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization")
    token = authz.split(" ", 1)[1]
    if not consteq(token, ADMIN_BEARER):
        raise HTTPException(status_code=403, detail="Forbidden")

@app.post("/keys", response_model=CreateKeyResponse, tags=["Admin"])
async def create_api_key(req: CreateKeyRequest, _: None = Depends(require_admin)):
    display, prefix, key_hash = make_key()
    async with app.state.db.acquire() as conn:
        metadata = json.dumps(req.metadata) if req.metadata is not None else None
        row = await conn.fetchrow(
            """
            INSERT INTO public.api_keys (owner_id, prefix, key_hash, metadata)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            """,
            req.owner_id, prefix, key_hash, metadata
        )
    return CreateKeyResponse(api_key=display, id=str(row["id"]))

@app.post("/keys/{key_id}/revoke", tags=["Admin"])
async def revoke_key(key_id: str, _: None = Depends(require_admin)):
    async with app.state.db.acquire() as conn:
        await conn.execute(
            "UPDATE public.api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL",
            key_id
        )
    return {"status": "ok"}
    
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))  # Cloud Run injects PORT
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=port,
    )
