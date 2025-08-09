from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import pyspark.sql.types as T

@dataclass(frozen=True)
class ColumnAddDiff:
    name: str
    data_type: T.DataType
    is_nullable: bool
    comment: str

@dataclass(frozen=True)
class ColumnDropDiff:
    name: str

@dataclass(frozen=True)
class NullabilityChangeDiff:
    name: str
    make_nullable: bool

@dataclass(frozen=True)
class PrimaryKeyDiff:
    set_columns: Optional[List[str]] = None  # None => no-op
    drop: bool = False

@dataclass(frozen=True)
class CommentsDiff:
    table_comment: Optional[str] = None
    column_comments: Dict[str, str] = field(default_factory=dict)

@dataclass(frozen=True)
class PropertiesDiff:
    to_set: Dict[str, str] = field(default_factory=dict)

@dataclass(frozen=True)
class TableDiff:
    is_create: bool
    # For create, carry full schema explicitly to avoid re-deriving
    create_schema: Optional[T.StructType] = None
    create_table_comment: str = ""
    create_table_properties: Dict[str, str] = field(default_factory=dict)
    create_primary_key_columns: List[str] = field(default_factory=list)
    create_column_comments: Dict[str, str] = field(default_factory=dict)

    # For align
    columns_to_add: List[ColumnAddDiff] = field(default_factory=list)
    columns_to_drop: List[ColumnDropDiff] = field(default_factory=list)
    nullability_changes: List[NullabilityChangeDiff] = field(default_factory=list)
    primary_key: PrimaryKeyDiff = PrimaryKeyDiff()
    comments: CommentsDiff = CommentsDiff()
    properties: PropertiesDiff = PropertiesDiff()
