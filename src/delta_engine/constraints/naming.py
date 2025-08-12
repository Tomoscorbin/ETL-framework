"""
Utilities for generating stable, SQL-safe names for Unity Catalog / Delta.

This module centralizes naming rules for:
- Constraint identifiers: builds deterministic primary-key names of the form
  ``pk_<catalog>_<schema>_<table>__<col1>_<col2>...``.
- Length safety: enforces the UC identifier limit (MAX_IDENTIFIER_LEN=128) by
  truncating and appending a deterministic 8-hex-character hash suffix.

Design guarantees
-----------------
- **Deterministic:** same inputs → same outputs across runs.
- **Escaped:** all identifier parts are backtick-escaped for safe SQL rendering.
- **Length-bounded:** `_truncate_with_hash` always returns a string whose length
  is ≤ `MAX_IDENTIFIER_LEN`, degrading gracefully for small limits.
- **Order-preserving:** `build_primary_key_name` preserves the order of columns
  (important if your PK semantics are order-sensitive).

Implementation notes
--------------------
- Hashing uses BLAKE2b with an 8-hex-character digest for compact, low-collision
  disambiguation in truncated names.
"""

from __future__ import annotations

from collections.abc import Sequence

MAX_IDENTIFIER_LEN = 128  # Databricks/Unity Catalog identifier limit


def _short_hash(*parts: str) -> str:
    """Deterministic 8-char hex hash for disambiguation in truncated identifiers."""
    # Use blake2b (fast, keyed-capable) and avoid S324/weak-hash warnings.
    import hashlib

    # 8 hex chars
    return hashlib.blake2b("|".join(parts).encode("utf-8"), digest_size=4).hexdigest()


def _truncate_with_hash(base: str, max_len: int = MAX_IDENTIFIER_LEN) -> str:
    """
    Truncate a long identifier to `max_len`, appending a suffix of the form ``_<hash>``.
    Guarantees the returned string length is <= `max_len` even for very small `max_len`.
    """
    if len(base) <= max_len:
        return base

    h = _short_hash(base)  # 8 chars
    # Prefer an underscore if we have room for it.
    if max_len <= len(h):
        # No room for underscore or base; return a trimmed hash.
        return h[:max_len]

    sep = "_"
    keep = max_len - len(sep) - len(h)
    if keep <= 0:
        # Not enough space for any of `base` + separator; drop the separator.
        return (base[: max_len - len(h)]) + h

    return f"{base[:keep]}{sep}{h}"


def build_primary_key_name(catalog: str, schema: str, table: str, columns: Sequence[str]) -> str:
    """
    Build a deterministic primary-key constraint name.

    Format (before truncation): ``pk_<catalog>_<schema>_<table>__<col1>_<col2>...``.
    The final name is truncated with a stable hash suffix to stay within `MAX_IDENTIFIER_LEN`.
    Column order is preserved and thus significant.
    """
    cols = "_".join(columns)
    base = f"pk_{catalog}_{schema}_{table}__{cols}"
    return _truncate_with_hash(base)
