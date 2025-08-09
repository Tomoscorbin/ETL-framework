"""Read live catalog state into domain models (TableState, CatalogState)."""

from __future__ import annotations

from typing import Dict, List, Sequence, cast

import pyspark.sql.types as T
from pyspark.sql import Row, SparkSession
from delta.tables import DeltaTable

from src.delta_engine.models import Table
from src.delta_engine.state.snapshot import CatalogState, ColumnState, TableState


class SparkCatalogReader:
    """
    Reads the current state of Delta tables from a live catalog into
    domain model objects (`CatalogState`, `TableState`, `ColumnState`).

    For each table, it checks existence, reads the schema, column comments,
    table comment, and table properties.
    """    
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark


    def snapshot(self, tables: Sequence[Table]) -> CatalogState:
        table_states_by_full_name: Dict[str, TableState] = {}
        for desired_table in tables:
            table_state = self._read_single_table_state(desired_table)
            table_states_by_full_name[table_state.full_name] = table_state
        return CatalogState(tables=table_states_by_full_name)
    


    def _read_single_table_state(self, desired_table: Table) -> TableState:
        full_name = self._full_name(desired_table)

        if not self._table_exists(full_name):
            return TableState(
                catalog_name=desired_table.catalog_name,
                schema_name=desired_table.schema_name,
                table_name=desired_table.table_name,
                exists=False,
            )

        table = self._read_table(full_name)

        struct_type = self._read_schema_struct(table)
        column_comments_by_name = self._read_column_comments(full_name)
        columns = self._merge_schema_and_comments(struct_type, column_comments_by_name)

        table_comment = self._read_table_comment(full_name)
        table_properties = self._read_table_properties(table, full_name)

        return TableState(
            catalog_name=desired_table.catalog_name,
            schema_name=desired_table.schema_name,
            table_name=desired_table.table_name,
            exists=True,
            columns=columns,
            table_comment=table_comment,
            table_properties=table_properties,
        )
    
    def _full_name(self, table: Table) -> str:
        return f"{table.catalog_name}.{table.schema_name}.{table.table_name}"

    def _table_exists(self, full_name: str) -> bool:
        return bool(self.spark.catalog.tableExists(full_name))

    def _read_table(self, full_name: str) -> DeltaTable:
        return DeltaTable.forName(self.spark, full_name)

    def _read_schema_struct(self, table: DeltaTable) -> T.StructType:
        return table.toDF().schema
    
    def _read_column_comments(self, full_name: str) -> Dict[str, str]:
        comments = {}
        columns = self.spark.catalog.listColumns(full_name)
        for column in columns:
            comments[column.name] = column.description or ""
        return comments
    
    def _merge_schema_and_comments(
        self,
        struct_type: T.StructType,
        comments_by_name: Dict[str, str],
    ) -> List[ColumnState]:
        merged: List[ColumnState] = []
        for field in struct_type.fields:
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
            table = self.spark.catalog.getTable(full_name)
            return table.description or ""
        except Exception:
            # If metadata permissions are restricted, return empty rather than fail.
            return ""

    def _read_table_properties(self, table: DeltaTable, full_name: str) -> Dict[str, str]:
        detail_df = delta_table.detail()
        cols = detail_df.columns
        config_col = next((c for c in cols if c.lower() == "configuration"), None)
        if config_col:
            row = detail_df.select(config_col).first()
            config_map = row[config_col] or {}
            return {str(k): str(v) for k, v in dict(config_map).items()}
