from __future__ import annotations

from collections.abc import Iterable

MAX_IDENTIFIER_LEN = 128  # Databricks/UC identifier limit


def three_part_to_qualified_name(name: tuple[str, str, str]) -> str:
    """Render ('catalog','schema','table') to `catalog.schema.table` with backticks."""
    return ".".join(f"`{part.replace('`', '``')}`" for part in name)


def _short_hash(*parts: str) -> str:
    """Deterministic 8-char hash used for truncation disambiguation."""
    # Use blake2b to avoid S324 warnings about sha1.
    import hashlib

    h = hashlib.blake2b("|".join(parts).encode("utf-8"), digest_size=8).hexdigest()
    return h[:8]


def _truncate_with_hash(base: str, max_len: int = MAX_IDENTIFIER_LEN) -> str:
    """Truncate long identifier with suffix _<hash> while staying within max_len."""
    if len(base) <= max_len:
        return base
    h = _short_hash(base)
    # keep space for "_" + hash
    keep = max_len - 1 - len(h)
    return f"{base[:keep]}_{h}"


def build_primary_key_name(catalog: str, schema: str, table: str, columns: Iterable[str]) -> str:
    """Deterministic PK name."""
    cols = "_".join(columns)
    base = f"pk_{catalog}_{schema}_{table}__{cols}"
    return _truncate_with_hash(base)
