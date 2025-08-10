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

from src.delta_engine.common_tpyes import ThreePartTableName
from src.delta_engine.models import Table
from src.delta_engine.state.snapshot import CatalogState, ColumnState, TableState, ConstraintState
from src.delta_engine.state.constraint_selects import (
    select_primary_key_name_for_table,
    select_foreign_key_names_for_source_table,
    select_referencing_foreign_keys_for_target_table,
)


class CatalogReader:
    """
    Reads live Delta catalog metadata into state model objects.

    Provides a `snapshot()` method for reading a set of tables, and internal
    helpers for reading individual table definitions and metadata.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    # ---------- public ----------

    def snapshot(self, tables: List[Table]) -> CatalogState:
        """Read schema + constraints for a set of tables."""
        states: dict[str, TableState] = {}
        for model in tables:
            table_state = self._read_table_state(model)
            states[table_state.full_name] = table_state

        return CatalogState(tables=states)

    # ---------- orchestration of a single table ----------

    def _read_table_state(self, model: Table) -> TableState:
        full_name = f"{model.catalog_name}.{model.schema_name}.{model.table_name}"
        if not self._exists(full_name):
            return TableState.empty(model.catalog_name, model.schema_name, model.table_name)

        delta = self._read_table(full_name)
        columns = self._read_columns(full_name, delta)
        table_comment = self._read_table_comment(full_name)
        table_properties = self._read_table_properties(delta)
        constraints = self._read_constraints_state_for_table(
            three_part=(model.catalog_name, model.schema_name, model.table_name),
            table_exists=True,
        )

        return TableState(
            catalog_name=model.catalog_name,
            schema_name=model.schema_name,
            table_name=model.table_name,
            exists=True,
            columns=columns,
            table_comment=table_comment,
            table_properties=table_properties,
            constraints=constraints,
        )

    # ---------- helpers ----------

    def _full_name(self, model: Table) -> str:
        return f"{model.catalog_name}.{model.schema_name}.{model.table_name}"

    def _exists(self, full_name: str) -> bool:
        return bool(self.spark.catalog.tableExists(full_name))

    def _read_table(self, full_name: str) -> DeltaTable:
        return DeltaTable.forName(self.spark, full_name)

    def _read_columns(self, full_name: str, delta: DeltaTable) -> List[ColumnState]:
        struct: T.StructType = delta.toDF().schema
        comments = self._read_column_comments(full_name)
        return self._merge_struct_and_comments(struct, comments)

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


    def _run(self, sql: str) -> List[Row]:
        return self.spark.sql(sql).collect()

    def _take_first_value(self, sql: str, column: str) -> Optional[str]:
        rows = self.spark.sql(sql).take(1)
        return rows[0][column] if rows else None


    # ---------- constraint helpers ----------

    def _read_constraints_state_for_table(self, three_part: ThreePartTableName) -> TableConstraintsState:
        pk_name = self._read_primary_key_name_for_table(three_part)
        fk_names = self._read_foreign_key_names_for_source_table(three_part)
        ref_fk_pairs = self.referencing_foreign_keys_for_target_table(three_part)

        # Sort deterministically
        ref_fk_pairs_sorted = tuple(sorted(ref_fk_pairs, key=lambda x: (x[0], x[1])))
        return TableConstraintsState(
            primary_key_name=pk_name,
            foreign_key_names=tuple(sorted(fk_names)),
            referencing_foreign_keys=ref_fk_pairs_sorted,
        )

    def _read_primary_key_name_for_table(self, three_part: ThreePartTableName) -> str | None:
        sql = select_primary_key_name_for_table(three_part)
        return self._first_value(sql, "name")

    def _read_foreign_key_names_for_source_table(self, three_part: ThreePartTableName) -> set[str]:
        sql = select_foreign_key_names_for_source_table(three_part)
        rows = self._run(sql)
        return {r["name"] for r in rows}

    def referencing_foreign_keys_for_target_table(self, three_part: ThreePartTableName) -> list[tuple[ThreePartTableName, str]]:
        sql = select_referencing_foreign_keys_for_target_table(three_part)
        rows = self._run(sql)
        return [((r["src_catalog"], r["src_schema"], r["src_table"]), r["name"]) for r in rows]