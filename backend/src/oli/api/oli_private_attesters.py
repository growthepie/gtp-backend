import os
import re
from functools import lru_cache
from typing import List


_ENV_VAR = "OLI_PRIVATE_ATTESTERS"
_HEX_ADDRESS_RE = re.compile(r"^[0-9a-f]{40}$")


def normalize_attester_address(address: str) -> str:
    value = str(address).strip().lower()
    if value.startswith("0x") or value.startswith("\\x"):
        value = value[2:]

    if not _HEX_ADDRESS_RE.match(value):
        raise ValueError(
            f"Invalid attester address in {_ENV_VAR}: {address!r}. "
            "Expected a 20-byte hex address."
        )
    return value


@lru_cache(maxsize=1)
def get_private_attester_hexes() -> List[str]:
    raw = os.getenv(_ENV_VAR, "")
    values = [part for part in re.split(r"[\s,;]+", raw) if part]
    seen = set()
    normalized = []
    for value in values:
        attester = normalize_attester_address(value)
        if attester not in seen:
            seen.add(attester)
            normalized.append(attester)
    return normalized


def get_private_attester_bytes() -> List[bytes]:
    return [bytes.fromhex(attester) for attester in get_private_attester_hexes()]


def private_attester_exclusion_sql(column_name: str = "attester", prefix: str = "AND") -> str:
    attesters = get_private_attester_hexes()
    if not attesters:
        return ""

    values = ", ".join(f"decode('{attester}', 'hex')" for attester in attesters)
    return f"{prefix} {column_name} NOT IN ({values})"


def reset_private_attester_cache() -> None:
    get_private_attester_hexes.cache_clear()
