from __future__ import annotations

from collections.abc import Iterable, Mapping

import pyspark.sql.types as T
from pyspark.sql import SparkSession

from src.delta_engine.sql import (
    sql_set_table_properties,
    sql_add_column,
    sql_drop_columns,
    sql_set_column_nullability,
    sql_set_column_comment,
    sql_set_table_comment,
    sql_add_primary_key,
    sql_drop_primary_key
)


class DeltaDDL:
    """Executes low-level CREATE/ALTER statements for Delta tables."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def create_table_if_not_exists(
        self,
        full_name: str,
        schema: T.StructType,
        table_comment: str = "",
    ) -> None:
        """Create a Delta table if it does not already exist."""
        from delta.tables import DeltaTable

        builder = DeltaTable.createIfNotExists(self.spark).tableName(full_name).addColumns(schema)
        if table_comment:
            builder = builder.comment(table_comment)
        builder.execute()

    def set_table_properties(self, full_name: str, props: Mapping[str, str]) -> None:
        """Set table properties."""
        for sql in sql_set_table_properties(full_name, props):
            self.spark.sql(sql)

    def add_column(self, full_name: str, name: str, dtype: T.DataType, comment: str = "") -> None:
        """Add a column to a table (and apply comment if provided)."""
        for sql in sql_add_column(full_name, name, dtype, comment):
            self.spark.sql(sql)

    def drop_columns(self, full_name: str, names: Iterable[str]) -> None:
        """Drop columns from a table."""
        for sql in sql_drop_columns(full_name, names):
            self.spark.sql(sql)

    def set_column_nullability(self, full_name: str, name: str, make_nullable: bool) -> None:
        """Change column nullability."""
        for sql in sql_set_column_nullability(full_name, name, make_nullable):
            self.spark.sql(sql)

    def set_column_comment(self, full_name: str, name: str, comment: str) -> None:
        """Set a comment on a column."""
        for sql in sql_set_column_comment(full_name, name, comment):
            self.spark.sql(sql)

    def set_table_comment(self, full_name: str, comment: str) -> None:
        """Set a comment on a table."""
        for sql in sql_set_table_comment(full_name, comment):
            self.spark.sql(sql)

    def add_primary_key(self, full_name: str, constraint_name: str, columns: list[str]) -> None:
        """Add a PRIMARY KEY constraint."""
        for sql in sql_add_primary_key(full_name, constraint_name, columns):
            self.spark.sql(sql)

    def drop_primary_key(self, full_name: str, constraint_name: str) -> None:
        """Drop a PRIMARY KEY constraint."""
        for sql in sql_drop_primary_key(full_name, constraint_name):
            self.spark.sql(sql)
