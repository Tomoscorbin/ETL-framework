from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass(frozen=True)
class ColumnState:
    """Observed column state."""
    name: str
    data_type: str
    is_nullable: bool
    comment: str = ""


@dataclass(frozen=True)
class TableState:
    """Observed table state."""
    catalog_name: str
    schema_name: str
    table_name: str
    exists: bool
    columns: List[ColumnState] = field(default_factory=list)
    table_comment: str = ""
    table_properties: Dict[str, str] = field(default_factory=dict)
    primary_key_columns: List[str] = field(default_factory=list)
    foreign_keys: List[ForeignKeyState] = field(default_factory=list)

    @property
    def full_name(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.table_name}"


@dataclass(frozen=True)
class CatalogState:
    """Snapshot of multiple tables keyed by full name."""
    tables: Dict[str, TableState]

    def get(
            self, 
            catalog_name: str, 
            schema_name: str, 
            table_name: str,
        ) -> TableState | None:
        return self.tables.get(f"{catalog_name}.{schema_name}.{table_name}")
    

@dataclass(frozen=True)
class ForeignKeyState:
    constraint_name: str
    source_columns: List[str]
    reference_table_name: str
    reference_columns: List[str]
