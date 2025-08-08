from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Mapping, Sequence
import pyspark.sql.types as T

@dataclass(frozen=True)
class CreateTable:
    catalog_name: str
    schema_name: str
    table_name: str
    schema_struct: T.StructType
    table_comment: str
    table_properties: Mapping[str, str]
    primary_key_columns: Sequence[str]
    column_comments: Mapping[str, str]

@dataclass(frozen=True)
class ColumnAdd:
    name: str
    data_type: T.DataType
    is_nullable: bool
    comment: str = ""

@dataclass(frozen=True)
class ColumnNullabilityChange:
    name: str
    make_nullable: bool  # True: DROP NOT NULL, False: SET NOT NULL

@dataclass(frozen=True)
class SetColumnComments:
    comments: Mapping[str, str]

@dataclass(frozen=True)
class SetTableComment:
    comment: str

@dataclass(frozen=True)
class SetTableProperties:
    properties: Mapping[str, str]

@dataclass(frozen=True)
class SetPrimaryKey:
    columns: Sequence[str]

@dataclass(frozen=True)
class DropPrimaryKey:
    pass

@dataclass(frozen=True)
class AlignTable:
    catalog_name: str
    schema_name: str
    table_name: str
    add_columns: List[ColumnAdd]
    change_nullability: List[ColumnNullabilityChange]
    set_column_comments: SetColumnComments | None
    set_table_comment: SetTableComment | None
    set_table_properties: SetTableProperties | None
    set_primary_key: SetPrimaryKey | None
    drop_primary_key: DropPrimaryKey | None

# ---------- Plan ----------
@dataclass(frozen=True)
class Plan:
    create_tables: List[CreateTable]
    align_tables: List[AlignTable]
    # foreign keys will come later
