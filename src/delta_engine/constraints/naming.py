from __future__ import annotations

from typing import Iterable, Tuple

MAX_IDENTIFIER_LEN = 128  # Databricks/UC identifier limit

def three_part_to_qualified_name(name: Tuple[str, str, str]) -> str:
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

def build_primary_key_name(three_part_table_name: Tuple[str, str, str], columns: Iterable[str]) -> str:
    """Deterministic PK name."""
    cat, sch, table = three_part_table_name
    cols = "_".join(columns)
    base = f"pk_{catalog}_{schema}_{table}__{cols}"
    return _truncate_with_hash(base)

def build_foreign_key_name(
    source_three_part_table_name: Tuple[str, str, str],
    source_columns: Iterable[str],
    target_three_part_table_name: Tuple[str, str, str],
    target_columns: Iterable[str],
) -> str:
    """Deterministic FK name derived from src/target table+columns."""
    scat, ssch, stbl = source_three_part_table_name
    tcat, tsch, ttbl = target_three_part_table_name
    scols = "_".join(source_columns)
    tcols = "_".join(target_columns)
    base = f"fk_{scat}_{ssch}_{stbl}__{scols}__ref__{tcat}_{tsch}_{ttbl}__{tcols}"
    return _truncate_with_hash(base)
