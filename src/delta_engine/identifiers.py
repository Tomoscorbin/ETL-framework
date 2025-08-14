from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, TypeAlias
from collections import defaultdict


_MAX_IDENTIFIER_LEN = 128                           # Unity Catalog identifier length limit
_INVALID_CHARACTER = re.compile(r"[^A-Za-z0-9]+")   # anything not alnum
_MULTI_UNDERSCORES = re.compile(r"_+")

@dataclass(frozen=True)
class FullyQualifiedTableName:
    """A three-part table name: catalog.schema.table."""
    catalog: str
    schema: str
    table: str

@dataclass(frozen=True)
class CatalogTargets:
    """Schema and table targets for a single catalog."""
    catalog: str
    schema_qualified_table_names: tuple[SchemaQualifiedTableName, ...]

def build_catalog_targets(
    identities: Iterable[FullyQualifiedTableName],
) -> tuple[CatalogTargets, ...]:
    """
    Group requested identities into per-catalog targets.

    - Preserves the original order of tables within each catalog.
    - Returns a tuple of CatalogTargets to keep callers from mutating the batches.
    """
    grouped = _group_schema_qualified_by_catalog(identities)
    targets: list[CatalogTargets] = []
    for catalog, schema_qualified_table_names in grouped.items():
        targets.append(
            CatalogTargets(
                catalog=catalog,
                schema_qualified_table_names=tuple(schema_qualified_table_names),
            )
        )
    return tuple(targets)

def _group_schema_qualified_by_catalog(
    identities: Iterable[FullyQualifiedTableName],
) -> dict[str, list[SchemaQualifiedTableName]]:
    """Build {catalog -> [SchemaQualifiedTableName, ...]} preserving input order."""
    grouped: dict[str, list[SchemaQualifiedTableName]] = defaultdict(list)
    for identity in identities:
        grouped[identity.catalog].append(
            SchemaQualifiedTableName(identity.schema, identity.table)
        )
    # Convert defaultdict to a plain dict to avoid leaking mutability semantics
    return dict(grouped)

# Identifier quoting/rendering (single source of truth)
def quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"

def render_fully_qualified_name(catalog: str, schema: str, table: str) -> str:
    """`catalog`.`schema`.`table`"""
    return quote_qualified_name(catalog, schema, table)

def fully_qualified_name_to_string(name: FullyQualifiedTableName) -> str:
    return ".".join([
        quote_identifier(name.catalog),
        quote_identifier(name.schema),
        quote_identifier(name.table)
    ])

def parse_fully_qualified(three_part: str) -> FullyQualifiedTableName:
    """
    Parse 'catalog.schema.table' or the backticked equivalent into a FullyQualifiedTableName.
    (Simple parser: strips backticks and splits on '.')
    """
    cleaned = three_part.replace("`", "")
    parts = cleaned.split(".")
    if len(parts) != 3 or any(p == "" for p in parts):
        raise ValueError(f"Expected three-part name 'catalog.schema.table', got: {three_part!r}")
    return FullyQualifiedTableName(parts[0], parts[1], parts[2])

def render_fully_qualified_name_from_parts(
    catalog: str,
    schema: str,
    table: str,
) -> str:
    """
    Return 'catalog.schema.table' with no quoting/escaping.
    """
    return fully_qualified_name_to_string(
        FullyQualifiedTableName(catalog=catalog, schema=schema, table=table)
    )

# Primary key builders
def _short_hash(*parts: str) -> str:
    """
    Deterministic 8-char hex hash for disambiguation in truncated identifiers.
    Uses BLAKE2b (fast, modern). The input is joined with '|' to keep boundaries.
    """
    joined = "|".join(parts).encode("utf-8")
    return hashlib.blake2b(joined, digest_size=4).hexdigest()  # 8 hex chars

def _sanitize_component(text: str) -> str:
    """
    Map an arbitrary string to a SQL-identifier-safe component using only [A-Za-z0-9_].
    - Invalid runs → underscore
    - Collapse multiple underscores → single underscore
    - Trim leading/trailing underscores
    Empty input returns an empty string.
    """
    if text is None:
        return ""
    s = _INVALID_CHARACTER.sub("_", str(text))
    s = _MULTI_UNDERSCORES.sub("_", s)
    return s.strip("_")


def _truncate_with_hash(base: str, max_len: int = _MAX_IDENTIFIER_LEN) -> str:
    """
    Truncate a long identifier to `max_len`, appending a suffix of the form `_hhhhhhhh`.
    Guarantees the returned string length is <= `max_len` even for very small limits.
    """
    if len(base) <= max_len:
        return base

    digest = _short_hash(base)  # 8 chars
    # If there is no room for an underscore, fall back to hash-only trimming.
    if max_len <= len(digest):
        return digest[:max_len]

    sep = "_"
    keep = max_len - len(sep) - len(digest)
    if keep <= 0:
        # Not enough space for any of base + separator; drop the separator.
        return base[: max_len - len(digest)] + digest

    return f"{base[:keep]}{sep}{digest}"

def build_primary_key_name(
    catalog: str,
    schema: str,
    table: str,
    columns: Sequence[str],
) -> str:
    """
    Build a deterministic primary-key constraint name.

    Pattern (before truncation):
        pk_<catalog>_<schema>_<table>__<col1>_<col2>_...

    - All parts are sanitised to [A-Za-z0-9_].
    - The final string is truncated with a stable hash suffix to stay within
      MAX_IDENTIFIER_LEN.
    - Column order is preserved.

    Raises
    ------
    ValueError
        If `columns` is empty. (Callers should only request a PK name when they
        actually intend to create/enforce a PK.)
    """
    if not columns:
        raise ValueError("Cannot build primary key name with no columns.")

    c = _sanitize_component(catalog)
    s = _sanitize_component(schema)
    t = _sanitize_component(table)
    cols_sanitised = [_sanitize_component(col) for col in columns]

    base = f"pk_{c}_{s}_{t}__" + "_".join(cols_sanitised)
    # Remove any accidental double underscores introduced by empty parts (defensive)
    base = base.replace("__", "__").strip("_")
    return _truncate_with_hash(base, MAX_IDENTIFIER_LEN)


def build_primary_key_name_for_identity(
    identity: FullyQualifiedTableName,
    columns: Sequence[str],
) -> str:
    """
    Convenience wrapper that accepts a FullyQualifiedTableName.
    """
    return build_primary_key_name(
        catalog=identity.catalog,
        schema=identity.schema,
        table=identity.table,
        columns=columns,
    )