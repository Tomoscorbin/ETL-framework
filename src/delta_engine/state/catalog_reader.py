"""
Reads a live catalog state into domain models.

This module defines `CatalogReader`, which inspects live Delta tables
and returns their state as `CatalogState`, `TableState`, and `ColumnState`
instances. It captures schema, column metadata, table comments, and table
properties without performing any modifications.
"""

from __future__ import annotations

from collections.abc import Sequence

import pyspark.sql.types as T
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from src.delta_engine.models import Table
from src.delta_engine.state.snapshot import CatalogState, ColumnState, TableState


class CatalogReader:
    """
    Reads live Delta catalog metadata into state model objects.

    Provides a `snapshot()` method for reading a set of tables, and internal
    helpers for reading individual table definitions and metadata.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    # ---------- public ----------

    def snapshot(self, tables: Sequence[Table]) -> CatalogState:
        """Read the current state for a set of tables."""
        states: dict[str, TableState] = {}
        for model in tables:
            state = self._read_table_state(model)
            states[state.full_name] = state
        return CatalogState(tables=states)

    # ---------- orchestration of a single table ----------

    def _read_table_state(self, model: Table) -> TableState:
        full_name = self._full_name(model)

        if not self._exists(full_name):
            return self._build_nonexistent_state(model)

        table = self._read_table(full_name)
        struct = self._read_struct(table)
        comments = self._read_column_comments(full_name)
        columns = self._merge_struct_and_comments(struct, comments)

        table_comment = self._read_table_comment(full_name)
        table_properties = self._read_table_properties(table)

        return self._build_existing_state(
            model=model,
            columns=columns,
            table_comment=table_comment,
            table_properties=table_properties,
        )

    # ---------- helpers ----------

    def _full_name(self, model: Table) -> str:
        return f"{model.catalog_name}.{model.schema_name}.{model.table_name}"

    def _exists(self, full_name: str) -> bool:
        return bool(self.spark.catalog.tableExists(full_name))

    def _read_table(self, full_name: str) -> DeltaTable:
        return DeltaTable.forName(self.spark, full_name)

    def _read_struct(self, delta: DeltaTable) -> T.StructType:
        # Delta API → DataFrame → schema
        return delta.toDF().schema

    def _read_column_comments(self, full_name: str) -> dict[str, str]:
        out: dict[str, str] = {}
        for col in self.spark.catalog.listColumns(full_name):
            out[col.name] = col.description or ""
        return out

    def _merge_struct_and_comments(
        self,
        struct: T.StructType,
        comments_by_name: dict[str, str],
    ) -> list[ColumnState]:
        merged: list[ColumnState] = []
        for field in struct.fields:
            merged.append(
                ColumnState(
                    name=field.name,
                    data_type=field.dataType,
                    is_nullable=field.nullable,
                    comment=comments_by_name.get(field.name, ""),
                )
            )
        return merged

    def _read_table_comment(self, full_name: str) -> str:
        try:
            return self.spark.catalog.getTable(full_name).description or ""
        except Exception:
            # Metadata permissions or missing description → empty string
            return ""

    def _read_table_properties(self, delta: DeltaTable) -> dict[str, str]:
        """
        Prefer Delta detail() 'configuration' map (no raw SQL).
        Fallback to empty dict if not present or unreadable.
        """
        try:
            detail_df = delta.detail()
            # Column name can vary in case; normalize lookup
            config_col = next((c for c in detail_df.columns if c.lower() == "configuration"), None)
            if not config_col:
                return {}
            row = detail_df.select(config_col).first()
            if not row:
                return {}
            config_map = row[config_col] or {}
            # Ensure plain dict[str, str]
            return {str(k): str(v) for k, v in dict(config_map).items()}
        except Exception:
            return {}

    # ---------- state constructors ----------

    def _build_nonexistent_state(self, model: Table) -> TableState:
        return TableState(
            catalog_name=model.catalog_name,
            schema_name=model.schema_name,
            table_name=model.table_name,
            exists=False,
            columns=[],
            table_comment="",
            table_properties={},
        )

    def _build_existing_state(
        self,
        model: Table,
        columns: list[ColumnState],
        table_comment: str,
        table_properties: dict[str, str],
    ) -> TableState:
        return TableState(
            catalog_name=model.catalog_name,
            schema_name=model.schema_name,
            table_name=model.table_name,
            exists=True,
            columns=columns,
            table_comment=table_comment,
            table_properties=table_properties,
        )
