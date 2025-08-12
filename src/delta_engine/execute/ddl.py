"""
Low-level DDL executor for Delta tables.

This module provides a thin, policy-free wrapper over Spark SQL statements.
Callers must pass fully-qualified, backticked table identifiers (e.g.
``catalog.schema.table`` escaped via `three_part_to_qualified_table_name`). All methods
are idempotent where the underlying Delta/Spark behavior is idempotent.

Design:
- No validation or business rules here; higher layers (planner/validator) enforce policy.
- Accepts normalized inputs and delegates SQL rendering to `src.delta_engine.sql.*`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

import pyspark.sql.types as T
from pyspark.sql import SparkSession

from src.delta_engine.sql import (
    sql_add_column,
    sql_add_primary_key,
    sql_drop_columns,
    sql_drop_primary_key,
    sql_set_column_comment,
    sql_set_column_nullability,
    sql_set_table_comment,
    sql_set_table_properties,
    sql_remove_table_properties
)


class DeltaDDL:
    """Execute low-level CREATE/ALTER statements for Delta tables via Spark SQL."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the DDL executor with a SparkSession."""
        self.spark = spark

    def _run(self, statement: str | None) -> None:
        """Execute a single SQL statement if provided (no-op for None/empty)."""
        if statement:
            self.spark.sql(statement)

    # ----- table creation / properties / comments -----

    def create_table_if_not_exists(
        self,
        qualified_table_name: str,
        schema: T.StructType,
        table_comment: str = "",
    ) -> None:
        """Create a Delta table if it does not already exist (uses Delta builder API)."""
        from delta.tables import DeltaTable

        builder = (
            DeltaTable.createIfNotExists(self.spark)
            .tableName(qualified_table_name)
            .addColumns(schema)
        )
        if table_comment:
            builder = builder.comment(table_comment)
        builder.execute()

    def set_table_properties(self, qualified_table_name: str, props: Mapping[str, str]) -> None:
        """Set table properties using ALTER TABLE ... SET TBLPROPERTIES."""
        self._run(sql_set_table_properties(qualified_table_name, props))


    def remove_table_properties(self, qualified_table_name: str, property_keys: Sequence[str]) -> None:
        """Unset one or more table properties via ALTER TABLE ... UNSET TBLPROPERTIES."""
        self._run(sql_remove_table_properties(qualified_table_name, property_keys))


    def add_column(
        self,
        qualified_table_name: str,
        column_name: str,
        dtype: T.DataType,
        comment: str = "",
    ) -> None:
        """Add a column to a table (comment is inlined when provided)."""
        self._run(sql_add_column(qualified_table_name, column_name, dtype, comment))

    def drop_columns(self, qualified_table_name: str, column_names: Iterable[str]) -> None:
        """Drop one or more columns using ALTER TABLE ... DROP COLUMNS."""
        names = list(column_names)
        self._run(sql_drop_columns(qualified_table_name, names))

    def set_column_nullability(
        self,
        qualified_table_name: str,
        column_name: str,
        make_nullable: bool,
    ) -> None:
        """Change a column's nullability via ALTER COLUMN DROP/SET NOT NULL."""
        self._run(sql_set_column_nullability(qualified_table_name, column_name, make_nullable))

    def set_column_comment(self, qualified_table_name: str, column_name: str, comment: str) -> None:
        """Set a column comment via COMMENT ON COLUMN."""
        self._run(sql_set_column_comment(qualified_table_name, column_name, comment))

    def set_table_comment(self, qualified_table_name: str, comment: str) -> None:
        """Set a table comment via COMMENT ON TABLE."""
        self._run(sql_set_table_comment(qualified_table_name, comment))

    def add_primary_key(
        self,
        qualified_table_name: str,
        constraint_name: str,
        column_names: list[str],
    ) -> None:
        """Add a PRIMARY KEY constraint via ALTER TABLE ... ADD CONSTRAINT."""
        self._run(sql_add_primary_key(qualified_table_name, constraint_name, column_names))

    def drop_primary_key(self, qualified_table_name: str, constraint_name: str) -> None:
        """Drop a PRIMARY KEY constraint via ALTER TABLE ... DROP CONSTRAINT."""
        self._run(sql_drop_primary_key(qualified_table_name, constraint_name))
