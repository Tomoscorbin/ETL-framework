"""
Identifier utilities for the Delta Engine.

This module defines:
- Canonical table-name dataclass: FullyQualifiedTableName.
- Helpers to quote, format, and parse qualified names.
- Deterministic builders for constraint identifiers (e.g., primary key names).

Conventions:
- Verbs: quote_*, format_*, parse_*, build_*.
- Use `full_table_name` for variables/parameters of type FullyQualifiedTableName.
- Sanitization for derived identifiers is lowercased and limited to [a-z0-9_].
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from dataclasses import dataclass


_MAX_IDENTIFIER_LEN = 128  # Unity Catalog identifier length limit
_INVALID_CHARACTER = re.compile(r"[^A-Za-z0-9_]+")  # anything not in [A-Za-z0-9_]
_MULTI_UNDERSCORES = re.compile(r"_+")


# -----------------------------
# Core name data structure
# -----------------------------


@dataclass(frozen=True)
class FullyQualifiedTableName:
    """Three-part table name: catalog.schema.table."""

    catalog: str
    schema: str
    table: str


# -----------------------------
# String helpers
# -----------------------------

def quote_identifier(identifier: str) -> str:
    """ Quote a single SQL identifier using backticks, doubling any embedded backticks."""
    text = str(identifier)
    return f"`{text.replace('`', '``')}`"

def quote_qualified_name(*parts: str) -> str:
    """
    Return a dot-delimited, backticked qualified name from the provided parts.

    Examples:
        quote_qualified_name("catalog", "schema", "table")
        -> "`catalog`.`schema`.`table`"

    Rules:
    - Reject None or empty parts.
    - Strips surrounding backticks on inputs to avoid double-quoting.
    """
    if not parts:
        raise ValueError("At least one name part must be provided.")

    cleaned_parts: list[str] = []
    for raw_part in parts:
        if raw_part is None:
            raise ValueError("Qualified name parts must not be None.")
        part = str(raw_part).strip()
        if part.startswith("`") and part.endswith("`") and len(part) >= 2:
            part = part[1:-1]  # remove surrounding backticks
        if part == "":
            raise ValueError("Qualified name parts must not be empty.")
        cleaned_parts.append(quote_identifier(part))
    return ".".join(cleaned_parts)


def quote_fully_qualified_table_name(full_table_name: FullyQualifiedTableName) -> str:
    """Backticked: `` `catalog`.`schema`.`table` ``."""
    return quote_qualified_name(
        full_table_name.catalog, full_table_name.schema, full_table_name.table
    )


def quote_fully_qualified_table_name_from_parts(
    catalog_name: str, schema_name: str, table_name: str
) -> str:
    """Backticked from parts: `` `catalog`.`schema`.`table` ``."""
    return quote_qualified_name(catalog_name, schema_name, table_name)


def format_fully_qualified_table_name(full_table_name: FullyQualifiedTableName) -> str:
    """Unquoted: 'catalog.schema.table'."""
    return f"{full_table_name.catalog}.{full_table_name.schema}.{full_table_name.table}"


def format_fully_qualified_table_name_from_parts(
    catalog_name: str, schema_name: str, table_name: str
) -> str:
    """Return the unquoted form from parts: 'catalog.schema.table'."""
    return f"{catalog_name}.{schema_name}.{table_name}"


def parse_fully_qualified_table_name(full_table_name_string: str) -> FullyQualifiedTableName:
    """
    Parse 'catalog.schema.table' (with or without backticks on parts) into components.

    This is a simple parser: it strips backticks and whitespace and splits on '.'.
    """
    cleaned = full_table_name_string.replace("`", "").strip()
    parts = [p.strip() for p in cleaned.split(".")]
    if len(parts) != 3 or any(p == "" for p in parts):
        raise ValueError(
            f"Expected three-part name 'catalog.schema.table', got: {full_table_name_string!r}"
        )
    return FullyQualifiedTableName(parts[0], parts[1], parts[2])


# -----------------------------
# Primary key name builder
# -----------------------------


def _short_hash(*parts: str) -> str:
    """
    Deterministic 8-char hex hash for disambiguation in truncated identifiers.
    Uses BLAKE2b. The input is joined with '|' to keep boundaries.
    """
    joined = "|".join(parts).encode("utf-8")
    return hashlib.blake2b(joined, digest_size=4).hexdigest()  # 8 hex chars


def _sanitize_component(text: str | None) -> str:
    """
    Map arbitrary text to an identifier-safe component using only [A-Za-z0-9_].
    - Invalid runs → underscore
    - Collapse multiple underscores → single underscore
    - Trim leading/trailing underscores
    - Lowercase for stability
    None or empty returns empty string.
    """
    if text is None:
        return ""
    s = _INVALID_CHARACTER.sub("_", str(text))
    s = _MULTI_UNDERSCORES.sub("_", s)
    s = s.strip("_")
    return s.lower()


def _truncate_with_hash(base: str, max_len: int = _MAX_IDENTIFIER_LEN) -> str:
    """
    Truncate a long identifier to `max_len`, appending a suffix of the form '_hhhhhhhh'.
    Guarantees the returned string length is <= `max_len` even for very small limits.
    """
    if len(base) <= max_len:
        return base

    digest = _short_hash(base)  # 8 chars
    if max_len <= len(digest):
        # No room for separator or base; return a truncated digest.
        return digest[:max_len]

    sep = "_"
    keep = max_len - len(sep) - len(digest)
    if keep <= 0:
        # Not enough space for any of base + separator; drop the separator.
        return base[: max_len - len(digest)] + digest

    return f"{base[:keep]}{sep}{digest}"


def build_primary_key_name(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    columns: Sequence[str],
) -> str:
    """
    Build a deterministic primary-key constraint name.

    Pattern (before truncation):
        pk_<catalog>_<schema>_<table>__<col1>_<col2>_...

    Rules:
    - All parts are sanitized to [a-z0-9_] and lowercased.
    - Column order is preserved.
    - The final string is truncated with a stable hash suffix to stay within
      `_MAX_IDENTIFIER_LEN`.

    Raises:
        ValueError: if `columns` is empty.
    """
    if not columns:
        raise ValueError("Cannot build primary key name with no columns.")

    c = _sanitize_component(catalog_name)
    s = _sanitize_component(schema_name)
    t = _sanitize_component(table_name)
    cols_sanitized = [x for x in (_sanitize_component(col) for col in columns) if x]

    # Assemble with a clear double-underscore separator between table and columns.
    base_parts = ["pk", c, s, t]
    left = "_".join([p for p in base_parts if p])  # drop any empty segments defensively
    right = "_".join(cols_sanitized)
    base = f"{left}__{right}" if right else left

    return _truncate_with_hash(base, _MAX_IDENTIFIER_LEN)


def build_primary_key_name_for_table_name(
    full_table_name: FullyQualifiedTableName,
    columns: Sequence[str],
) -> str:
    """Convenience wrapper accepting a FullyQualifiedTableName."""
    return build_primary_key_name(
        catalog_name=full_table_name.catalog,
        schema_name=full_table_name.schema,
        table_name=full_table_name.table,
        columns=columns,
    )
