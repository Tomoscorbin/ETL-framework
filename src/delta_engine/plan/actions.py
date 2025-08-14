from __future__ import annotations
from dataclasses import dataclass
from typing import Mapping, Tuple
import pyspark.sql.types as T
from src.delta_engine.identifiers import FullyQualifiedTableName

@dataclass(frozen=True)
class Action:
    table: FullyQualifiedTableName

@dataclass(frozen=True)
class ColumnSpec:
    name: str
    data_type: T.DataType
    is_nullable: bool
    comment: str = ""

@dataclass(frozen=True)
class AddColumns(Action):
    columns: Tuple[ColumnSpec, ...]

@dataclass(frozen=True)
class AlterColumnNullability(Action):
    column: str
    is_nullable: bool

@dataclass(frozen=True)
class SetColumnComments(Action):
    comments_by_name: Mapping[str, str]

@dataclass(frozen=True)
class SetTableComment(Action):
    comment: str

@dataclass(frozen=True)
class SetTableProperties(Action):
    properties: Mapping[str, str]

@dataclass(frozen=True)
class CreatePrimaryKey(Action):
    name: str
    columns: Tuple[str, ...]

@dataclass(frozen=True)
class DropPrimaryKey(Action):
    pass


@dataclass(frozen=True)
class CreateTable(Action):
    columns: Tuple[ColumnSpec, ...]
    properties: Mapping[str, str]
    comment: str

@dataclass(frozen=True)
class AlignTable(Action):
    catalog_name: str
    schema_name: str
    table_name: str

    add_columns: Tuple[AddColumnSpec, ...] = ()
    drop_columns: Tuple[DropColumnSpec, ...] = ()
    change_nullability: Tuple[NullabilityChangeSpec, ...] = ()
    add_primary_key: AddPrimaryKey | None = None
    drop_primary_key: DropPrimaryKey | None = None
    set_column_comments: ColumnCommentsSpec | None = None
    set_table_comment: TableCommentSpec | None = None
    set_table_properties: TablePropertiesSpec | None = None