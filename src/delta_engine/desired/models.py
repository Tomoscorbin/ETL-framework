from __future__ import annotations
from dataclasses import dataclass
from typing import Mapping, Tuple
from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.models import Column

@dataclass(frozen=True)
class DesiredTable:
    fully_qualified_table_name: FullyQualifiedTableName
    columns: Tuple[Column, ...]
    primary_key_columns: Tuple[str, ...] | None = None
    primary_key_name_override: str | None = None
    table_comment: str | None = None
    table_properties: Mapping[str, str] | None = None

@dataclass(frozen=True)
class DesiredCatalog:
    tables: Tuple[DesiredTable, ...]
