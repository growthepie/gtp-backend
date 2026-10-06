import json
import os
import re
from functools import lru_cache
from typing import Dict, Iterable, List


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
def get_private_attester_names() -> Dict[str, str]:
    raw = os.getenv(_ENV_VAR, "").strip()
    if raw.startswith("{"):
        # Dictionary keys are addresses; values are human-readable names.
        values = json.loads(raw)
        if not isinstance(values, dict):
            raise ValueError(f"{_ENV_VAR} must be an address-to-name object")
    else:
        values = {
            part: "0x" + normalize_attester_address(part)
            for part in re.split(r"[\s,;]+", raw) if part
        }
    normalized = {}
    for value, name in values.items():
        attester = normalize_attester_address(value)
        normalized.setdefault(attester, name)
    return normalized


@lru_cache(maxsize=1)
def get_private_attester_hexes() -> List[str]:
    return list(get_private_attester_names())


def private_attester_notification_suffix(attesters: Iterable[str]) -> str:
    private_names = get_private_attester_names()
    names = dict.fromkeys(
        private_names[address]
        for attester in attesters
        if (address := normalize_attester_address(attester)) in private_names
    )
    return f" INTERNAl by {', '.join(names)}" if names else ""


def get_private_attester_bytes() -> List[bytes]:
    return [bytes.fromhex(attester) for attester in get_private_attester_hexes()]


def require_private_attesters() -> List[bytes]:
    """Public readers/exporters must fail closed when configuration is absent."""
    attesters = get_private_attester_bytes()
    if not attesters:
        raise RuntimeError(f"{_ENV_VAR} must contain at least one private attester")
    return attesters


def private_attester_exclusion_sql(column_name: str = "attester", prefix: str = "AND") -> str:
    require_private_attesters()
    attesters = get_private_attester_hexes()

    values = ", ".join(f"decode('{attester}', 'hex')" for attester in attesters)
    return f"{prefix} {column_name} NOT IN ({values})"


def reset_private_attester_cache() -> None:
    get_private_attester_names.cache_clear()
    get_private_attester_hexes.cache_clear()
