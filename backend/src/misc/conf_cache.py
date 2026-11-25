"""
Per-user on-disk cache for the config pickles served from the API CDN (main_conf.pkl, da_conf.pkl).

Airflow parses DAG files and runs tasks in separate short-lived processes, so an in-memory cache
does not survive between them. Without this cache every get_main_config() call (including at DAG
parse time, every ~30s per file) re-downloaded the pickle from CloudFront (~10k requests/day).

Behaviour:
- A local copy younger than GTP_CONF_CACHE_TTL seconds (default 900) is used without any request.
- Older copies are revalidated with If-None-Match (ETag) -> CloudFront answers 304 with no body.
- If the fetch fails and a local copy exists, that (stale) copy is used with a warning.
- The cache is best-effort: any cache read/write problem falls back to a plain fetch.
- prime_conf_cache() stores a freshly uploaded pickle locally right away (api_conf_pickles DAG),
  since the CloudFront invalidation is asynchronous and could otherwise serve the old one.

Cache file format: one file per pickle, '<etag>\\n<pickle bytes>', replaced atomically so the
ETag and body can never get out of sync.
"""
import os
import time
import pickle
import hashlib
import tempfile

import requests

CACHE_DIR = os.getenv("GTP_CONF_CACHE_DIR", os.path.join(os.path.expanduser("~"), ".cache", "gtp_conf"))
CACHE_TTL = int(os.getenv("GTP_CONF_CACHE_TTL", "900"))
REQUEST_TIMEOUT = 30
BASE_URL = "https://api.growthepie.xyz"


def _path(api_version: str, filename: str) -> str:
    return os.path.join(CACHE_DIR, f"{api_version}_{filename}")


def _read_cache(path: str):
    """Return (etag, body, age_seconds) or None if there is no usable cache file."""
    try:
        age = time.time() - os.path.getmtime(path)
        with open(path, "rb") as f:
            raw = f.read()
        etag, body = raw.split(b"\n", 1)
        return etag.decode(), body, age
    except Exception:
        return None


def _write_cache(path: str, etag: str, body: bytes):
    """Atomically replace the cache file. Best-effort: failures are only logged."""
    tmp = None
    try:
        os.makedirs(CACHE_DIR, mode=0o700, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=CACHE_DIR, prefix=".tmp_")
        with os.fdopen(fd, "wb") as f:
            f.write((etag or "").encode() + b"\n" + body)
        os.replace(tmp, path)
    except Exception as e:
        print(f"WARNING: could not write conf cache {path}: {e}")
        if tmp and os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _fetch(url: str, headers: dict = None):
    response = requests.get(url, headers=headers or {}, timeout=REQUEST_TIMEOUT)
    if response.status_code == 304:
        return response, None
    response.raise_for_status()
    return response, pickle.loads(response.content)  # validate before caching


def load_conf_pickle(filename: str, api_version: str = "v1"):
    """Return the unpickled object for {BASE_URL}/{api_version}/{filename}, using the disk cache."""
    url = f"{BASE_URL}/{api_version}/{filename}"
    path = _path(api_version, filename)
    cached = _read_cache(path)

    if cached:
        etag, body, age = cached
        if age < CACHE_TTL:
            try:
                return pickle.loads(body)
            except Exception:
                cached = None  # corrupt cache -> fetch below

    try:
        headers = {"If-None-Match": cached[0]} if cached and cached[0] else {}
        response, obj = _fetch(url, headers)
        if obj is None:  # 304: unchanged upstream -> restart TTL window
            try:
                obj = pickle.loads(cached[1])
                os.utime(path)
                return obj
            except Exception:
                response, obj = _fetch(url)  # cache vanished/corrupt mid-request -> plain fetch
        _write_cache(path, response.headers.get("ETag", ""), response.content)
        return obj
    except Exception as e:
        if cached:
            print(f"WARNING: could not refresh {filename} ({e}); using cached copy from {int(cached[2])}s ago")
            return pickle.loads(cached[1])
        raise


def prime_conf_cache(filename: str, local_path: str, api_version: str = "v1"):
    """Store a just-uploaded pickle in the local cache (S3/CloudFront ETag of a single-part upload = MD5)."""
    try:
        with open(local_path, "rb") as f:
            body = f.read()
        _write_cache(_path(api_version, filename), f'"{hashlib.md5(body).hexdigest()}"', body)
    except Exception as e:
        print(f"WARNING: could not prime conf cache for {filename}: {e}")
