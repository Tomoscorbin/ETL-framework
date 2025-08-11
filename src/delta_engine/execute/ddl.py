from __future__ import annotations

from collections.abc import Iterable, Mapping

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
)


class DeltaDDL:
    """Execute low-level CREATE/ALTER statements for Delta tables via Spark SQL."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the DDL executor with a SparkSession."""
        self.spark = spark

    def _run(self, sql: str | None) -> None:
        """Execute a single SQL statement if provided."""
        if sql:
            self.spark.sql(sql)

    def create_table_if_not_exists(
        self,
        full_name: str,
        schema: T.StructType,
        table_comment: str = "",
    ) -> None:
        """Create a Delta table if it does not already exist (uses Delta builder API)."""
        from delta.tables import DeltaTable

        builder = DeltaTable.createIfNotExists(self.spark).tableName(full_name).addColumns(schema)
        if table_comment:
            builder = builder.comment(table_comment)
        builder.execute()

    def set_table_properties(self, full_name: str, props: Mapping[str, str]) -> None:
        """Set table properties using ALTER TABLE ... SET TBLPROPERTIES."""
        self._run(sql_set_table_properties(full_name, props))

    def add_column(
        self,
        full_name: str,
        name: str,
        dtype: T.DataType,
        comment: str = "",
    ) -> None:
        """Add a column to a table (comment is inlined when provided)."""
        self._run(sql_add_column(full_name, name, dtype, comment))

    def drop_columns(self, full_name: str, names: Iterable[str]) -> None:
        """Drop one or more columns using ALTER TABLE ... DROP COLUMNS."""
        self._run(sql_drop_columns(full_name, names))

    def set_column_nullability(
        self,
        full_name: str,
        name: str,
        make_nullable: bool,
    ) -> None:
        """Change a column's nullability via ALTER COLUMN DROP/SET NOT NULL."""
        self._run(sql_set_column_nullability(full_name, name, make_nullable))

    def set_column_comment(self, full_name: str, name: str, comment: str) -> None:
        """Set a column comment via COMMENT ON COLUMN."""
        self._run(sql_set_column_comment(full_name, name, comment))

    def set_table_comment(self, full_name: str, comment: str) -> None:
        """Set a table comment via COMMENT ON TABLE."""
        self._run(sql_set_table_comment(full_name, comment))

    def add_primary_key(
        self,
        full_name: str,
        constraint_name: str,
        columns: list[str],
    ) -> None:
        """Add a PRIMARY KEY constraint via ALTER TABLE ... ADD CONSTRAINT."""
        self._run(sql_add_primary_key(full_name, constraint_name, columns))

    def drop_primary_key(self, full_name: str, constraint_name: str) -> None:
        """Drop a PRIMARY KEY constraint via ALTER TABLE ... DROP CONSTRAINT."""
        self._run(sql_drop_primary_key(full_name, constraint_name))
