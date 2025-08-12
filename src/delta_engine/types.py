from typing import Protocol, TypeAlias


ThreePartTableName: TypeAlias = tuple[str, str, str]

class HasTableIdentity(Protocol):
    """Protocol for objects that expose catalog/schema/table identity."""

    catalog_name: str
    schema_name: str
    table_name: str
