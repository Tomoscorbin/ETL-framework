"""
Adapter: Physical Schema Reader

Reads the physical schema (column name, data type, nullability) and existence
for Delta tables, one table at a time.

Flow
----
1) Inputs are FullyQualifiedTableName values (catalog.schema.table).
2) For each table:
   - Check existence via the Spark catalog.
   - If it exists and schema is requested, load StructType via Delta and convert
     fields into ColumnState (comment left empty for later merge).
3) On any failure:
   - Emit a SnapshotWarning with Aspect.SCHEMA.
   - Treat the table as non-existent for this read and return no columns.

Notes:
-----
- Only existence and physical columns are handled here; comments/properties/PK are separate.
"""

from __future__ import annotations

from typing import NamedTuple

import pyspark.sql.types as T
from pyspark.sql import SparkSession

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.state.adapters._delta_executor import (
    check_table_exists,
    load_table_struct,
)
from src.delta_engine.state.ports import Aspect, SnapshotWarning
from src.delta_engine.state.states import ColumnState


class SchemaBatchReadResult(NamedTuple):
    """
    Aggregated result of reading existence + physical schema for a set of tables.

    Attributes:
    ----------
    existence_by_table :
        {FullyQualifiedTableName -> bool} existence per table.
    columns_by_table :
        {FullyQualifiedTableName -> tuple[ColumnState, ...]} physical columns for
        existing tables when requested; empty tuple otherwise.
    warnings :
        Non-fatal warnings encountered while reading.
    """

    existence_by_table: dict[FullyQualifiedTableName, bool]
    columns_by_table: dict[FullyQualifiedTableName, tuple[ColumnState, ...]]
    warnings: list[SnapshotWarning]


class SchemaReader:
    """
    Read table existence and physical schema, one table at a time.
    """

    def __init__(self, spark: SparkSession) -> None:
        """Store the Spark session used to access catalog/Delta metadata."""
        self.spark = spark

    def read_schema(
        self,
        full_table_names: tuple[FullyQualifiedTableName, ...],
        include_schema: bool,
    ) -> SchemaBatchReadResult:
        """
        Read existence (always) and optionally physical schema for each table.

        - Existence is checked via Spark catalog.
        - If a table exists and `include_schema` is True, load StructType via Delta
          and convert to ColumnState values (comments left empty).
        - On any exception, emit a SnapshotWarning and treat the table as non-existent
          with no columns in this result.
        """
        existence_by_table: dict[FullyQualifiedTableName, bool] = {}
        columns_by_table: dict[FullyQualifiedTableName, tuple[ColumnState, ...]] = {}
        warnings: list[SnapshotWarning] = []

        for full_table_name in full_table_names:
            # 1) Existence
            try:
                exists_flag = check_table_exists(
                    self.spark,
                    catalog_name=full_table_name.catalog,
                    schema_name=full_table_name.schema,
                    table_name=full_table_name.table,
                )
                existence_by_table[full_table_name] = bool(exists_flag)
            except Exception as error:
                warnings.append(
                    SnapshotWarning.from_exception(
                        aspect=Aspect.SCHEMA,
                        error=error,
                        full_table_name=full_table_name,
                        prefix="Failed to check table existence",
                    )
                )
                existence_by_table[full_table_name] = False
                columns_by_table[full_table_name] = tuple()
                continue

            # 2) Short-circuit if absent or schema not requested
            if not existence_by_table[full_table_name] or not include_schema:
                columns_by_table[full_table_name] = tuple()
                continue

            # 3) Load StructType and convert to ColumnState
            try:
                struct = load_table_struct(
                    self.spark,
                    catalog_name=full_table_name.catalog,
                    schema_name=full_table_name.schema,
                    table_name=full_table_name.table,
                )
                columns_by_table[full_table_name] = _column_states_from_struct(struct)
            except Exception as error:
                warnings.append(
                    SnapshotWarning.from_exception(
                        aspect=Aspect.SCHEMA,
                        error=error,
                        full_table_name=full_table_name,
                        prefix="Failed to load table schema",
                    )
                )
                # Existence stays True; columns empty for this read.
                columns_by_table[full_table_name] = tuple()

        return SchemaBatchReadResult(
            existence_by_table=existence_by_table,
            columns_by_table=columns_by_table,
            warnings=warnings,
        )


# ---------- helpers ----------


def _column_states_from_struct(struct: T.StructType) -> tuple[ColumnState, ...]:
    """
    Convert a Spark StructType into immutable ColumnState objects.
    Column comments are not set here; they are merged later by the builder.
    """
    out: list[ColumnState] = []
    for field in struct.fields:
        out.append(
            ColumnState(
                name=field.name,
                data_type=field.dataType,
                is_nullable=field.nullable,
                comment="",  # merged later
            )
        )
    return tuple(out)
