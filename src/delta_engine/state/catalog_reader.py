"""Read live catalog state into domain models."""

from __future__ import annotations

from typing import List, Optional
from collections.abc import Sequence

import pyspark.sql.types as T
from delta.tables import DeltaTable
from pyspark.sql import Row, SparkSession

from src.delta_engine.common_types import ThreePartTableName
from src.delta_engine.models import Table
from src.delta_engine.state.constraint_selects import (
    select_primary_key_columns_for_table,
    select_primary_key_name_for_table,
)
from src.delta_engine.state.states import (
    CatalogState,
    ColumnState,
    PrimaryKeyState,
    TableState,
)


class CatalogReader:
    """Read live Delta catalog metadata into state model objects."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    # ---------- public ----------
    def snapshot(self, tables: Sequence[Table]) -> CatalogState:
        """Read schema information for a set of tables."""
        states: dict[str, TableState] = {}
        for model in tables:
            table_state = self._read_table_state(model)
            states[table_state.full_name] = table_state
        return CatalogState(tables=states)

    # ---------- orchestration of a single table ----------
    def _read_table_state(self, model: Table) -> TableState:
        full_name = f"{model.catalog_name}.{model.schema_name}.{model.table_name}"
        if not self._exists(full_name):
            return TableState.empty(
                model.catalog_name, model.schema_name, model.table_name
            )

        delta = self._read_table(full_name)
        columns = self._read_columns(full_name, delta)
        table_comment = self._read_table_comment(full_name)
        table_properties = self._read_table_properties(delta)
        primary_key = self._read_primary_key_state(
            (model.catalog_name, model.schema_name, model.table_name)
        )

        return TableState(
            catalog_name=model.catalog_name,
            schema_name=model.schema_name,
            table_name=model.table_name,
            exists=True,
            columns=columns,
            table_comment=table_comment,
            table_properties=table_properties,
            primary_key=primary_key,
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
        """Read Delta table properties, or return an empty dict on failure."""
        try:
            detail_df = delta.detail()
            config_col = next(
                (c for c in detail_df.columns if c.lower() == "configuration"),
                None,
            )
            if not config_col:
                return {}
            row = detail_df.select(config_col).first()
            if not row:
                return {}
            config_map = row[config_col] or {}
            return {str(k): str(v) for k, v in dict(config_map).items()}
        except Exception:
            return {}

    def _read_primary_key_state(
        self, three_part: ThreePartTableName
    ) -> PrimaryKeyState | None:
        name = self._read_primary_key_name_for_table(three_part)
        if not name:
            return None
        cols = self._read_primary_key_columns_for_table(three_part)
        return PrimaryKeyState(name=name, columns=tuple(cols))

    def _read_primary_key_name_for_table(
        self, three_part: ThreePartTableName
    ) -> str | None:
        sql = select_primary_key_name_for_table(three_part)
        return self._take_first_value(sql, "name")

    def _read_primary_key_columns_for_table(
        self, three_part: ThreePartTableName
    ) -> list[str]:
        sql = select_primary_key_columns_for_table(three_part)
        rows = self._run(sql)
        return [r["column_name"] for r in sorted(rows, key=lambda r: r["ordinal_position"])]

    def _run(self, sql: str) -> List[Row]:
        return self.spark.sql(sql).collect()

    def _take_first_value(self, sql: str, column: str) -> Optional[str]:
        rows = self.spark.sql(sql).take(1)
        return rows[0][column] if rows else None

