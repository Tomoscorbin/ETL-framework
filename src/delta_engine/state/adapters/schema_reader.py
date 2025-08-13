"""
Adapter: Physical Schema Reader

This module reads the **physical schema** (column name, data type, nullability)
for Delta tables, one table at a time. It also reports table existence.

High-level flow
---------------
1) Inputs are **FullyQualifiedTableName** values (catalog.schema.table).
2) For each table:
   - Check existence via Spark catalog.
   - If the table exists and schema is requested, load its StructType via Delta
     and convert fields into `ColumnState` (comments left empty for later merge).
3) On any failure for a table:
   - Emit a **SnapshotWarning** with `Aspect.SCHEMA`.
   - Treat the table as non-existent for this read and return no columns.

Design notes
------------
- Existence and physical schema are read here; comments/properties/constraints
  are handled by separate readers.
- Column comments are **not** surfaced here; they are merged later by the builder.
"""

from __future__ import annotations

from typing import NamedTuple
import pyspark.sql.types as T
from pyspark.sql import SparkSession

from src.delta_engine.state.ports import SnapshotWarning, Aspect
from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.state.states import ColumnState
from src.delta_engine.state.adapters._delta_executor import (
    check_table_exists,
    load_table_struct,
)


class SchemaBatchReadResult(NamedTuple):
    """
    Aggregated result of reading existence + physical schema for a set of tables.

    Attributes
    ----------
    existence_by_table:
        {FullyQualifiedTableName -> bool} whether the table exists (as far as the
        Spark catalog reports in this session/context).
    columns_by_table:
        {FullyQualifiedTableName -> tuple[ColumnState, ...]} physical columns for
        existing tables when requested; empty tuple otherwise.
    warnings:
        A list of SnapshotWarning objects for failures encountered while reading.
    """
    existence_by_table: dict[FullyQualifiedTableName, bool]
    columns_by_table: dict[FullyQualifiedTableName, tuple[ColumnState, ...]]
    warnings: list[SnapshotWarning]


class SchemaReader:
    """
    Read table existence and physical schema, one table at a time.

    Inputs
    ------
    FullyQualifiedTableName values (catalog.schema.table).

    Outputs
    -------
    SchemaBatchReadResult with:
      - existence flags
      - physical columns (if requested and the table exists)
      - warnings for read failures
    """

    def __init__(self, spark: SparkSession) -> None:
        """Store the Spark session used to access catalog/Delta metadata."""
        self.spark = spark

    def read_schema(
        self,
        table_names: tuple[FullyQualifiedTableName, ...],
        include_schema: bool,
    ) -> SchemaBatchReadResult:
        """
        Read existence (always) and optionally physical schema for each table.

        Behavior
        --------
        - Existence is checked via Spark catalog.
        - If a table exists and `include_schema` is True, load StructType via Delta
          and convert to ColumnState values (comments left empty).
        - On any exception (existence check or schema load), emit a SnapshotWarning
          and treat the table as non-existent with no columns in this result.

        Returns
        -------
        SchemaBatchReadResult
        """
        existence_by_table: dict[FullyQualifiedTableName, bool] = {}
        columns_by_table: dict[FullyQualifiedTableName, tuple[ColumnState, ...]] = {}
        warnings: list[SnapshotWarning] = []

        for name in table_names:
            # 1) Existence
            try:
                exists = check_table_exists(
                    self.spark,
                    catalog=name.catalog,
                    schema=name.schema,
                    table=name.table,
                )
                existence_by_table[name] = bool(exists)
            except Exception as error:
                warnings.append(
                    SnapshotWarning.from_exception(
                        aspect=Aspect.SCHEMA,
                        error=error,
                        table=name,
                        prefix="Failed to check table existence",
                    )
                )
                existence_by_table[name] = False
                columns_by_table[name] = tuple()
                continue

            # 2) Short-circuit if absent or schema not requested
            if not existence_by_table[name] or not include_schema:
                columns_by_table[name] = tuple()
                continue

            # 3) Load StructType and convert to ColumnState
            try:
                struct = load_table_struct(
                    self.spark,
                    catalog=name.catalog,
                    schema=name.schema,
                    table=name.table,
                )
                columns_by_table[name] = _column_states_from_struct(struct)
            except Exception as error:
                warnings.append(
                    SnapshotWarning.from_exception(
                        aspect=Aspect.SCHEMA,
                        error=error,
                        table=name,
                        prefix="Failed to load table schema",
                    )
                )
                
                # Treat as temporarily unreadable; existence stays True, columns empty.
                columns_by_table[name] = tuple()

        return SchemaBatchReadResult(
            existence_by_table=existence_by_table,
            columns_by_table=columns_by_table,
            warnings=warnings,
        )


# ---------- helpers (tiny, explicit) ----------

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
