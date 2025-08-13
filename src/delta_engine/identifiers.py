from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, TypeAlias
from collections import defaultdict

@dataclass(frozen=True)
class FullyQualifiedTableName:
    """A three-part table name: catalog.schema.table."""
    catalog: str
    schema: str
    table: str

@dataclass(frozen=True)
class SchemaQualifiedTableName:
    """A two-part table name that is only meaningful within a catalog."""
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

def make_fully_qualified(catalog: str, two_part: SchemaQualifiedTableName) -> TableIdentity:
    """Combine catalog + schema.table into a full identity."""
    return TableIdentity(catalog=catalog, schema=two_part.schema, table=two_part.table)

# Identifier quoting/rendering (single source of truth)
def quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"

def qualify_fully_qualified_name(name: TableIdentity) -> str:
    return ".".join([
        quote_identifier(name.catalog),
        quote_identifier(name.schema),
        quote_identifier(name.table)
    ])

def qualify_schema_qualified_name(name: SchemaQualifiedTableName) -> str:
    return ".".join([quote_identifier(name.schema), quote_identifier(name.table)])

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