"""
Executes CREATE TABLE actions in Delta Lake.

This module defines `CreateExecutor`, which applies `CreateTable` actions
by creating the table if it does not exist, setting properties, and applying
column comments.
"""

from pyspark.sql import SparkSession

from src.delta_engine.actions import CreateTable

from .ddl import DeltaDDL


class CreateExecutor:
    """Executes a `CreateTable` action against the catalog."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the executor."""
        self.ddl = DeltaDDL(spark)

    def apply(self, action: CreateTable) -> None:
        """
        Apply a CREATE TABLE action.

        Steps:
          1. Create the table with schema and comment if it does not exist.
          2. Set table properties (e.g., delta.columnMapping.mode).
          3. Apply column comments.
          4. Add PRIMARY KEY
        """
        full = f"{action.catalog_name}.{action.schema_name}.{action.table_name}"

        # 1) Create skeleton with schema + comment
        self.ddl.create_table_if_not_exists(full, action.schema_struct, action.table_comment)

        # 2) Properties (e.g., delta.columnMapping.mode = name)
        self.ddl.set_table_properties(full, action.table_properties)

        # 3) Column comments
        for col, comment in (action.column_comments or {}).items():
            if comment:
                self.ddl.set_column_comment(full, col, comment)

        # 4) Primary key (if specified)
        if action.primary_key:
            self.ddl.add_primary_key(
                full,
                action.primary_key.name,
                list(action.primary_key.columns),
            )

