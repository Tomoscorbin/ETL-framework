from __future__ import annotations
from dataclasses import dataclass, field
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
class ColumnDrop:
    name: str

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
    set_column_comments: SetColumnComments | None = None
    set_table_comment: SetTableComment | None = None
    set_table_properties: SetTableProperties | None = None
    drop_primary_key: DropPrimaryKey | None = None
    set_primary_key: SetPrimaryKey | None = None

    add_columns: List[ColumnAdd] = field(default_factory=list)
    change_nullability: List[ColumnNullabilityChange] = field(default_factory=list)
    drop_columns: List[ColumnDrop] = field(default_factory=list)

# ---------- Plan ----------
@dataclass(frozen=True)
class Plan:
    create_tables: List[CreateTable]
    align_tables: List[AlignTable]
    # foreign keys will come later
