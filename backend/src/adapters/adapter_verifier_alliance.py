"""Incremental Verifier Alliance v2 export -> OLI, with a durable signed outbox.

The state directory must be on persistent local storage and owned by one worker.
Only projected Parquet columns are fetched; source code and bytecode are omitted.
"""

import io
import json
import logging
import re
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from xml.etree import ElementTree

import pyarrow.parquet as pq
import pyarrow.compute as pc
import requests
import yaml
from jsonschema import Draft202012Validator
from sqlalchemy import text

LOG = logging.getLogger(__name__)
EXPORT_URL = "https://storage.googleapis.com/verifier-alliance-parquet-export"
COLUMNS = {
    "compiled_contracts": ["id", "name", "compiler", "version", "language"],
    "contract_deployments": ["id", "chain_id", "address", "transaction_hash", "block_number", "deployer"],
    "verified_contracts": ["id", "deployment_id", "compilation_id", "created_at", "created_by",
                           "creation_match", "runtime_match", "creation_metadata_match", "runtime_metadata_match"],
}
ZERO_UID = "0x" + "00" * 32
MAX_DATABASE_BYTES = 90_000_000_000  # Decimal GB; leave room below the user's 100 GB ceiling.
TAG_DEFINITIONS_URL = "https://raw.githubusercontent.com/openlabelsinitiative/OLI/main/1_label_schema/tags/tag_definitions.yml"


class UnknownVerifierError(ValueError):
    pass


class StorageLimitError(RuntimeError):
    pass


def allowed_verifiers(session, oli=None):
    if oli is not None:
        definitions = oli.tag_definitions
    else:
        response = session.get(TAG_DEFINITIONS_URL, timeout=(15, 60))
        response.raise_for_status()
        definitions = {tag["tag_id"]: tag for tag in yaml.safe_load(response.text)["tags"]}
    values = definitions["source_code_verified"]["schema"]["enum"]
    if not values or not all(isinstance(value, str) and value for value in values):
        raise ValueError("OLI source_code_verified enum is missing or invalid")
    return set(values)


def notify_once(state, key, message, notify):
    """At most daily per issue; failed delivery must not mark the alert sent."""
    LOG.error(message)
    if notify is None:
        return
    previous = state.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
    if previous and time.time() - float(previous[0]) < 86400:
        return
    notify(message)
    with state.db:
        state.db.execute("INSERT OR REPLACE INTO metadata VALUES (?, ?)", (key, str(time.time())))


def verifier_roles(session, file):
    with RangeReader(session, f"{EXPORT_URL}/{file['key']}", file["size"], file["etag"]) as reader:
        parquet = pq.ParquetFile(reader)
        roles = set()
        for batch in parquet.iter_batches(columns=["created_by"], batch_size=100000, use_threads=False):
            roles.update(pc.unique(batch.column(0)).to_pylist())
        return roles


def preflight_verifiers(state, session, files, allowed, aliases, notify=None):
    """Check pending files before building the large cache or submitting anything."""
    mapping, unknown = {}, set()
    for file in files:
        if state.progress(file)[1]:
            continue
        cached = state.db.execute("SELECT etag, roles FROM file_roles WHERE key=?", (file["key"],)).fetchone()
        if cached and cached[0] == file["etag"]:
            roles = json.loads(cached[1])
        else:
            roles = list(verifier_roles(session, file))
            with state.db:
                state.db.execute("INSERT OR REPLACE INTO file_roles VALUES (?, ?, ?)",
                                 (file["key"], file["etag"], json.dumps(roles)))
        for role in roles:
            value = aliases.get(role, role)
            if value not in allowed:
                unknown.add(str(role))
            else:
                mapping[role] = value
    if unknown:
        names = ", ".join(sorted(unknown))
        notify_once(state, "unknown:" + names,
                    f"Verifier Alliance sync BLOCKED: source_code_verified needs new OLI entities: {names}. "
                    f"Add the genuine verifier identities to {TAG_DEFINITIONS_URL}, then rerun. "
                    "No unsupported contracts were attested or skipped; do not map them to a different verifier.", notify)
        raise UnknownVerifierError(f"Unsupported verifier identities: {names}")
    return mapping


def json_value(value, tag=None):
    """Match the plain-text representation used by OLI's labels view."""
    if tag == "code_compiler" and isinstance(value, str):
        # Sourcify uses compiler-version; the initial importer used a space.
        # Treat those separators as equivalent, preserving the complete version.
        return re.sub(r"^([A-Za-z][A-Za-z0-9_]*) (?=v?\d+\.)", r"\1-", value)
    if tag in {"deployment_tx", "deployer_address"} and isinstance(value, str):
        return value.lower()
    return value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))


def hex_value(value, length):
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).hex()
    value = str(value).lower().removeprefix("0x").removeprefix("\\x")
    if not re.fullmatch(r"[0-9a-f]{%d}" % (length * 2), value):
        raise ValueError(f"Expected {length}-byte hex value")
    return "0x" + value


def make_label(verification, deployment, compilation, verifier_map):
    chain = int(deployment["chain_id"])
    if chain <= 0 or not (verification["creation_match"] or verification["runtime_match"]):
        raise ValueError("Invalid chain or verification without a bytecode match")
    verifier = verifier_map.get(verification["created_by"])
    if not verifier:
        raise UnknownVerifierError(f"Verifier has not passed schema preflight: {verification['created_by']}")
    tags = {"is_contract": True, "source_code_verified": verifier}
    if compilation.get("name"):
        tags["contract_name"] = compilation["name"]
    language = compilation["language"].lower()
    if language in {"solidity", "vyper", "yul", "fe", "huff", "stylus"}:
        tags["code_language"] = language
    if compilation.get("compiler") and compilation.get("version"):
        tags["code_compiler"] = f"{compilation['compiler']}-{compilation['version']}"
    for source, target, length in (("transaction_hash", "deployment_tx", 32), ("deployer", "deployer_address", 20)):
        if deployment.get(source) is not None:
            tags[target] = hex_value(deployment[source], length)
    if deployment.get("block_number") is not None:
        block = int(deployment["block_number"])
        if block < 0:
            raise ValueError("Negative deployment block")
        tags["deployment_block"] = block
    tags["_source"] = "https://verifieralliance.org/"
    # No standard tags exist for match quality, verification time, or compilation IDs.
    tags["_comment"] = json.dumps({"verifier_alliance": verification}, default=str, sort_keys=True)
    return {"address": hex_value(deployment["address"], 20), "chain_id": f"eip155:{chain}", "tags": tags}


class RangeReader(io.RawIOBase):
    """Seekable HTTP reader that pins every request to the listed object ETag."""

    def __init__(self, session, url, size, etag):
        self.session, self.url, self.size, self.etag = session, url, size, etag
        self.position = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=0):
        position = offset + (self.position if whence == 1 else self.size if whence == 2 else 0)
        if whence not in (0, 1, 2) or position < 0:
            raise ValueError("Invalid seek")
        self.position = position
        return position

    def read(self, size=-1):
        size = min(self.size - self.position, size if size >= 0 else self.size)
        if size <= 0:
            return b""
        end = self.position + size - 1
        with self.session.get(self.url, headers={"Range": f"bytes={self.position}-{end}",
                              "If-Match": self.etag}, timeout=(15, 180)) as response:
            response.raise_for_status()
            if response.status_code != 206 or len(response.content) != size:
                raise RuntimeError("Export endpoint did not honor the byte range")
            data = response.content
        self.position += len(data)
        return data


def list_files(session, table):
    files, marker = [], ""
    while True:
        response = session.get(EXPORT_URL, params={"prefix": f"v2/{table}/", "marker": marker,
                                                   "max-keys": 1000}, timeout=(15, 60))
        response.raise_for_status()
        root = ElementTree.fromstring(response.content)
        for item in root.findall("{*}Contents"):
            key = item.findtext("{*}Key")
            if re.fullmatch(rf"v2/{table}/{table}_\d+_\d+\.parquet", key):
                files.append({"key": key, "etag": item.findtext("{*}ETag"),
                              "size": int(item.findtext("{*}Size"))})
        if root.findtext("{*}IsTruncated") != "true":
            break
        next_marker = root.findtext("{*}NextMarker")
        if not next_marker or next_marker == marker:
            raise RuntimeError("Truncated export listing without a progressing marker")
        marker = next_marker
    if not files:
        raise RuntimeError(f"No export files for {table}")
    return sorted(files, key=lambda f: int(f["key"].rsplit("_", 2)[1]))


def parquet_batches(session, file, table, batch_size=500):
    with RangeReader(session, f"{EXPORT_URL}/{file['key']}", file["size"], file["etag"]) as reader:
        parquet = pq.ParquetFile(reader)
        missing = set(COLUMNS[table]) - set(parquet.schema_arrow.names)
        if missing:
            raise ValueError(f"Missing export columns: {missing}")
        for batch in parquet.iter_batches(batch_size=batch_size, columns=COLUMNS[table], use_threads=False):
            yield batch.to_pylist()


class State:
    def __init__(self, path, max_bytes=MAX_DATABASE_BYTES):
        self.db = sqlite3.connect(path)
        if not 0 < max_bytes <= MAX_DATABASE_BYTES:
            self.db.close()
            raise ValueError("Database limit must be positive and at most 90 GB")
        page_size = self.db.execute("PRAGMA page_size").fetchone()[0]
        limit = int(max_bytes // page_size)
        if self.db.execute("PRAGMA page_count").fetchone()[0] > limit:
            self.db.close()
            raise StorageLimitError("Existing state exceeds the 90 GB database limit; refusing to grow it")
        # SQLite enforces this inside each transaction, even if the next batch is large.
        self.db.execute(f"PRAGMA max_page_count={limit}")
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("PRAGMA temp_store=MEMORY")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS dimensions (
                kind TEXT, id TEXT, data TEXT NOT NULL, PRIMARY KEY(kind, id));
            CREATE TABLE IF NOT EXISTS files (
                key TEXT PRIMARY KEY, etag TEXT NOT NULL, offset INTEGER NOT NULL, complete INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS sent (
                chain TEXT, address TEXT, tag TEXT, value TEXT,
                PRIMARY KEY(chain, address, tag, value));
            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY CHECK(id = 1), payload TEXT NOT NULL, labels TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS file_roles (key TEXT PRIMARY KEY, etag TEXT, roles TEXT NOT NULL);
        """)

    def progress(self, file):
        row = self.db.execute("SELECT etag, offset, complete FROM files WHERE key=?", (file["key"],)).fetchone()
        return (row[1], bool(row[2])) if row and row[0] == file["etag"] else (0, False)

    def checkpoint(self, file, offset, complete=False):
        self.db.execute("INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?)",
                        (file["key"], file["etag"], offset, int(complete)))

    def dimension(self, kind, identifier):
        row = self.db.execute("SELECT data FROM dimensions WHERE kind=? AND id=?", (kind, str(identifier))).fetchone()
        if not row:
            raise RuntimeError(f"Export dependency missing: {kind}/{identifier}; retry after next export")
        return json.loads(row[0])


def existing_labels(engine, labels):
    """One indexed lookup per batch, across all attesters, including Sourcify."""
    if not labels:
        return set()
    # OLI's labels view exposes lowercase 0x-prefixed TEXT addresses, unlike
    # the bytea addresses used in several growthepie analytics tables.
    addresses = list({hex_value(label["address"], 20) for label in labels})
    chains = list({label["chain_id"] for label in labels})
    with engine.connect() as connection:
        rows = connection.execute(text("""
            SELECT address, chain_id, tag_id, tag_value
            FROM public.labels
            WHERE address = ANY(CAST(:addresses AS text[])) AND chain_id = ANY(:chains)
              AND tag_id NOT IN ('_source', '_comment')
        """), {"addresses": addresses, "chains": chains})
        return {(str(r.chain_id), hex_value(r.address, 20), r.tag_id, json_value(r.tag_value, r.tag_id)) for r in rows}


def missing_labels(labels, existing, state):
    result = []
    for label in labels:
        tags = {}
        for tag, value in label["tags"].items():
            if tag.startswith("_"):
                continue
            key = (label["chain_id"], label["address"], tag, json_value(value, tag))
            legacy_value = (re.sub(r"^([A-Za-z][A-Za-z0-9_]*)-(?=v?\d+\.)", r"\1 ", key[-1])
                            if tag == "code_compiler" else key[-1])
            if key in existing or state.db.execute(
                    "SELECT 1 FROM sent WHERE chain=? AND address=? AND tag=? AND value IN (?, ?)",
                    (*key, legacy_value)).fetchone():
                continue
            tags[tag] = value
            existing.add(key)  # Deduplicate repeated verifications within this batch.
        if tags:
            # Every new attestation explicitly carries the verification provider,
            # even when this one value already exists in OLI.
            tags["source_code_verified"] = label["tags"]["source_code_verified"]
            tags.update({k: v for k, v in label["tags"].items() if k.startswith("_")})
            result.append({**label, "tags": tags})
    return result


def reconcile_receipts(state, engine, limit=4000):
    """Drop only exact receipts already visible in OLI; scan fairly across runs.

    Free SQLite pages are reused. No full VACUUM or second database-sized copy.
    Never expire a receipt by age: OLI indexing may be behind.
    """
    saved = state.db.execute("SELECT value FROM metadata WHERE key='receipt_cursor'").fetchone()
    cursor = int(saved[0]) if saved else 0
    rows = state.db.execute("SELECT rowid, chain, address, tag, value FROM sent WHERE rowid>? ORDER BY rowid LIMIT ?",
                            (cursor, limit)).fetchall()
    if not rows:
        rows = state.db.execute("SELECT rowid, chain, address, tag, value FROM sent ORDER BY rowid LIMIT ?", (limit,)).fetchall()
    if not rows:
        return 0
    labels = [{"chain_id": chain, "address": address} for _, chain, address, _, _ in rows]
    visible = existing_labels(engine, labels)
    confirmed = [(rowid,) for rowid, chain, address, tag, value in rows
                 if (chain, address, tag, json_value(value, tag)) in visible]
    with state.db:
        state.db.executemany("DELETE FROM sent WHERE rowid=?", confirmed)
        state.db.execute("INSERT OR REPLACE INTO metadata VALUES ('receipt_cursor', ?)", (str(rows[-1][0]),))
    return len(confirmed)


def sign_labels(oli, labels):
    if not oli.tag_definitions:
        raise RuntimeError("OLI tag definitions unavailable; refusing unvalidated attestations")
    signed = []
    validators = {tag: Draft202012Validator(definition["schema"])
                  for tag, definition in oli.tag_definitions.items()}
    for label in labels:
        if not label["tags"].get("source_code_verified"):
            raise UnknownVerifierError("Every attestation must include source_code_verified")
        # The SDK's tag validator currently warns for several schema errors.
        # Fail here instead of retaining an invalid signed batch in the outbox.
        for tag, value in label["tags"].items():
            if tag not in validators:
                raise ValueError(f"OLI tag is not registered: {tag}")
            validators[tag].validate(value)
        oli.validator.validate_label_correctness(label["address"], label["chain_id"], label["tags"], ZERO_UID, auto_fix=False)
        data = oli.utils_other.encode_label_data(f"{label['chain_id']}:{label['address']}", label["tags"])
        attestation = oli.offchain.build_offchain_attestation(
            recipient="0x0000000000000000000000000000000000000001",
            schema=oli.oli_label_pool_schema, data=data, ref_uid=ZERO_UID)
        for key in ("time", "expirationTime"):
            attestation["sig"]["message"][key] = str(attestation["sig"]["message"][key])
        attestation["sig"]["domain"]["chainId"] = str(attestation["sig"]["domain"]["chainId"])
        signed.append(attestation)
    return signed


def flush_outbox(state, oli):
    row = state.db.execute("SELECT payload, labels FROM outbox WHERE id=1").fetchone()
    if not row:
        return
    payload, labels = map(json.loads, row)
    allowed = oli.tag_definitions["source_code_verified"]["schema"]["enum"]
    if any(label["tags"].get("source_code_verified") not in allowed for label in labels):
        raise UnknownVerifierError("Pending outbox has a missing/unsupported verifier; retain it and reconcile before retry")
    response = oli.api.post_bulk_attestations(payload)
    response.raise_for_status()
    result = response.json()
    if result.get("failed_validation") or result.get("accepted", 0) + result.get("duplicates", 0) != len(payload):
        raise RuntimeError(f"OLI did not accept the whole batch: {result}")
    # A crash before this commit replays identical signed UIDs, never new attestations.
    with state.db:
        state.db.executemany("INSERT OR IGNORE INTO sent VALUES (?, ?, ?, ?)",
            [(label["chain_id"], label["address"], tag, json_value(value, tag))
             for label in labels for tag, value in label["tags"].items() if not tag.startswith("_")])
        state.db.execute("DELETE FROM outbox WHERE id=1")


@contextmanager
def locked_state(directory):
    import fcntl

    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "sync.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if sum(p.stat().st_size for p in directory.rglob("*") if p.is_file()) >= 95_000_000_000:
            raise StorageLimitError("State directory has reached 95 GB; move backups elsewhere and inspect state before retry")
        state = State(directory / "state.sqlite")
        try:
            yield state
        finally:
            state.db.close()


def sync(directory, engine, oli=None, *, dry_run=True, max_batches=100, verifier_map=None, notify=None):
    try:
        return _sync(directory, engine, oli, dry_run=dry_run, max_batches=max_batches,
                     verifier_map=verifier_map, notify=notify)
    except sqlite3.DatabaseError as exc:
        if getattr(exc, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL:
            if notify and not dry_run:
                notify("Verifier Alliance sync BLOCKED: SQLite storage limit (90 GB) or disk capacity reached. "
                       "No capacity increase is allowed; retain the outbox and reconcile receipts/cache before retrying.")
        raise
    except StorageLimitError as exc:
        if notify and not dry_run:
            notify(f"Verifier Alliance sync BLOCKED: {exc}")
        raise
    except UnknownVerifierError as exc:
        # Preflight reports specific roles with throttling. A legacy outbox may
        # fail separately before new data is read.
        if "outbox" in str(exc) and notify and not dry_run:
            notify(f"Verifier Alliance sync BLOCKED: {exc}")
        raise


def _sync(directory, engine, oli=None, *, dry_run=True, max_batches=100, verifier_map=None, notify=None):
    """First run backfills; subsequent runs read new/changed export objects.

    max_batches bounds verification processing, not the initial dimension import.
    Dry runs cache dimensions but never advance verification cursors or sign/send.
    """
    if max_batches < 1:
        raise ValueError("max_batches must be positive")
    aliases = verifier_map or {}
    if not dry_run and oli is None:
        raise ValueError("Live sync requires an OLI client")
    stats = {"scanned": 0, "attestations": 0, "tags": 0, "unknown_verifiers": {}}
    with locked_state(directory) as state, requests.Session() as session:
        allowed = allowed_verifiers(session, oli)
        # List and check verifier roles first, before any costly cache construction.
        verified_files = list_files(session, "verified_contracts")
        verifier_map = preflight_verifiers(state, session, verified_files, allowed, aliases,
                                           None if dry_run else notify)
        if not dry_run:
            flush_outbox(state, oli)
            for _ in range(25):
                if not reconcile_receipts(state, engine):
                    break
        # List verifications first: later dimension listings can then include their dependencies.
        for table in ("compiled_contracts", "contract_deployments"):
            for file in list_files(session, table):
                offset, complete = state.progress(file)
                if complete:
                    continue
                LOG.info("Caching %s", file["key"])
                count = 0
                for rows in parquet_batches(session, file, table, batch_size=10000):
                    count += len(rows)
                    if count <= offset:
                        continue
                    serialized = []
                    for row in rows:
                        row = {k: (hex_value(v, len(v)) if isinstance(v, bytes) else str(v) if v is not None else None)
                               for k, v in row.items()}
                        serialized.append((table, row["id"], json.dumps(row)))
                    with state.db:
                        state.db.executemany("INSERT OR REPLACE INTO dimensions VALUES (?, ?, ?)", serialized)
                        state.checkpoint(file, count)
                with state.db:
                    state.checkpoint(file, count, True)
        batches = 0
        for file in verified_files:
            offset, complete = state.progress(file)
            if complete:
                continue
            count = 0
            for rows in parquet_batches(session, file, "verified_contracts"):
                count += len(rows)
                if count <= offset:
                    continue
                labels = []
                for row in rows:
                    labels.append(make_label(row, state.dimension("contract_deployments", row["deployment_id"]),
                                             state.dimension("compiled_contracts", row["compilation_id"]), verifier_map))
                missing = missing_labels(labels, existing_labels(engine, labels), state)
                stats["scanned"] += len(rows)
                stats["attestations"] += len(missing)
                stats["tags"] += sum(sum(not k.startswith("_") for k in l["tags"]) for l in missing)
                if missing and not dry_run:
                    payload = sign_labels(oli, missing)
                    with state.db:
                        state.db.execute("INSERT INTO outbox VALUES (1, ?, ?)",
                                         (json.dumps(payload), json.dumps(missing)))
                    flush_outbox(state, oli)
                if not dry_run:
                    with state.db:
                        state.checkpoint(file, count)
                    stats["receipts_pruned"] = stats.get("receipts_pruned", 0) + reconcile_receipts(state, engine)
                batches += 1
                used_bytes = state.db.execute("PRAGMA page_count").fetchone()[0] * state.db.execute("PRAGMA page_size").fetchone()[0]
                if used_bytes >= 80_000_000_000:
                    notify_once(state, "storage-warning", "Verifier Alliance state is at least 80 GB. "
                                "The 90 GB database cap will stop growth; inspect cache/receipt reconciliation.",
                                None if dry_run else notify)
                LOG.info("Verifier Alliance progress: %s", stats)
                if batches >= max_batches:
                    return {**stats, "bounded": True}
            if not dry_run:
                with state.db:
                    state.checkpoint(file, count, True)
    return {**stats, "bounded": False}
