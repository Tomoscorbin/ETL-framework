from __future__ import annotations

from typing import Dict, List, Sequence, cast

import pyspark.sql.types as T
from pyspark.sql import Row, SparkSession
from delta.tables import DeltaTable

from src.table_management.models import Table
from src.table_management.state.snapshot import CatalogState, ColumnState, TableState


def escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


class SparkCatalogReader:
    """
    CatalogReader using DeltaTable and Spark Catalog.
    - Existence via spark.catalog.tableExists
    - Schema via DeltaTable.toDF().schema
    - Column comments via spark.catalog.listColumns
    - Table comment via spark.catalog.getTable
    - Properties via DeltaTable.detail()['configuration'] (fallback to SHOW TBLPROPERTIES)
    - Primary keys via INFORMATION_SCHEMA (no Python API available)
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

        delta_table = self._read_delta_table(full_name)

        struct_type = self._read_schema_struct(delta_table)
        column_comments_by_name = self._read_column_comments(full_name)
        columns = self._merge_schema_and_comments(struct_type, column_comments_by_name)

        table_comment = self._read_table_comment(full_name)
        table_properties = self._read_table_properties(delta_table, full_name)
        primary_key_columns = self._read_primary_key_columns(
            catalog_name=desired_table.catalog_name,
            schema_name=desired_table.schema_name,
            table_name=desired_table.table_name,
        )

        return TableState(
            catalog_name=desired_table.catalog_name,
            schema_name=desired_table.schema_name,
            table_name=desired_table.table_name,
            exists=True,
            columns=columns,
            table_comment=table_comment,
            table_properties=table_properties,
            primary_key_columns=primary_key_columns,
        )
    
    def _full_name(self, table: Table) -> str:
        return f"{table.catalog_name}.{table.schema_name}.{table.table_name}"

    def _table_exists(self, full_name: str) -> bool:
        return bool(self.spark.catalog.tableExists(full_name))

    def _read_delta_table(self, full_name: str) -> DeltaTable:
        return DeltaTable.forName(self.spark, full_name)

    def _read_schema_struct(self, delta_table: DeltaTable) -> T.StructType:
        return delta_table.toDF().schema
    
    def _read_column_comments(self, full_name: str) -> Dict[str, str]:
        comments: Dict[str, str] = {}
        for column in self.spark.catalog.listColumns(full_name):
            comments[column.name] = column.comment or ""
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

    def _read_table_properties(self, delta_table: DeltaTable, full_name: str) -> Dict[str, str]:
        try:
            detail_df = delta_table.detail()
            lower_columns = [c.lower() for c in detail_df.columns]
            if "configuration" in lower_columns:
                # Use the exact column name from the DataFrame (preserve case)
                config_col_name = next(c for c in detail_df.columns if c.lower() == "configuration")
                row = detail_df.select(config_col_name).collect()[0]
                config_map = cast(Optional[dict], row[config_col_name]) or {}
                return {str(k): str(v) for k, v in config_map.items()}
        except Exception:
            # Fall through to SHOW TBLPROPERTIES
            pass

        # Fallback only if detail() path failed or configuration is missing.
        rows: List[Row] = self.spark.sql(f"SHOW TBLPROPERTIES {full_name}").collect()
        return {r["key"]: r["value"] for r in rows}

    def _read_primary_key_columns(self, catalog_name: str, schema_name: str, table_name: str) -> List[str]:
        sql = f"""
          SELECT kcu.column_name
          FROM {catalog_name}.information_schema.table_constraints tc
          JOIN {catalog_name}.information_schema.key_column_usage kcu
            ON  tc.constraint_catalog = kcu.constraint_catalog
            AND tc.constraint_schema  = kcu.constraint_schema
            AND tc.constraint_name    = kcu.constraint_name
          WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = '{escape_sql_literal(schema_name)}'
            AND tc.table_name   = '{escape_sql_literal(table_name)}'
          ORDER BY kcu.ordinal_position
        """
        rows = self.spark.sql(sql).collect()
        return [r["column_name"] for r in rows]