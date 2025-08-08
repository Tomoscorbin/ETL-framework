"""Utility functions used across models (identifiers, names, SQL safety)."""

import hashlib

MANAGED_CONSTRAINT_PREFIX = "fk_"
_MAX_IDENTIFIER_LENGTH = 128


def quote_ident(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def quote_qualified_name(catalog: str, schema: str, table: str) -> str:
    return ".".join(quote_ident(x) for x in (catalog, schema, table))


def escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _truncate_with_hash(base: str, max_len: int) -> str:
    if len(base) <= max_len:
        return base
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:8]
    return f"{base[:max_len - 1 - len(h)]}_{h}"


def short_hash(*parts: str) -> str:
    """Deterministic 8-char hash for salt/disambiguation."""
    joined = "|".join(parts)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()[:8]


def build_fk_name_minimal(
    *,
    source_catalog: str,
    source_table: str,
    target_catalog: str,
    target_table: str,
    salt: str | None = None,
    prefix: str = MANAGED_CONSTRAINT_PREFIX,
) -> str:
    """
    Minimal FK name: fk_<src_catalog>_<src_table>__ref__<tgt_catalog>_<tgt_table>
    If `salt` is provided, it's appended as _<salt>.
    """
    base = f"{prefix}{source_catalog}_{source_table}__ref__{target_catalog}_{target_table}".lower()
    if salt:
        base = f"{base}_{salt}"
    return _truncate_with_hash(base, _MAX_IDENTIFIER_LENGTH)
