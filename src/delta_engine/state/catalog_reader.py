"""Read live catalog state into domain models (TableState, CatalogState)."""

from __future__ import annotations

from collections.abc import Sequence

import pyspark.sql.types as T
from delta.tables import DeltaTable
from pyspark.sql import Row, SparkSession

from src.delta_engine.models import Table
from src.delta_engine.sql import (
    sql_select_primary_key_columns_for_table,
    sql_select_primary_key_name_for_table,
)
from src.delta_engine.state.states import (
    CatalogState,
    ColumnState,
    PrimaryKeyState,
    TableState,
)
from src.delta_engine.types import ThreePartTableName


class CatalogReader:
    """Read live Delta catalog metadata into state model objects."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark: SparkSession = spark

    # ---------- public ----------

    def snapshot(self, tables: Sequence[Table]) -> CatalogState:
        """Read schema, comments, properties, and PK metadata for a set of tables."""
        states: dict[str, TableState] = {}
        for model in tables:
            full_name_unescaped = self._full_name(model)
            table_state = self._read_table_state(model)
            states[full_name_unescaped] = table_state
        return CatalogState(tables=states)

    # ---------- orchestration of a single table ----------

    def _read_table_state(self, model: Table) -> TableState:
        """
        Read live metadata for a single table.

        Returns an empty `TableState` when the table does not exist.
        """
        full_name_unescaped = self._full_name(model)
        if not self._exists(full_name_unescaped):
            return TableState.empty(model.catalog_name, model.schema_name, model.table_name)

        delta = self._read_table(full_name_unescaped)
        columns = self._read_columns(full_name_unescaped, delta)
        table_comment = self._read_table_comment(full_name_unescaped)
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
        """Return the unescaped three-part name `catalog.schema.table`."""
        return f"{model.catalog_name}.{model.schema_name}.{model.table_name}"

    def _exists(self, full_name_unescaped: str) -> bool:
        """Return True if the table exists in the catalog."""
        return bool(self.spark.catalog.tableExists(full_name_unescaped))

    def _read_table(self, full_name_unescaped: str) -> DeltaTable:
        """Return a `DeltaTable` handle for the given table name."""
        return DeltaTable.forName(self.spark, full_name_unescaped)

    def _read_columns(self, full_name_unescaped: str, delta: DeltaTable) -> list[ColumnState]:
        """
        Read columns (name, type, nullability, comment) from Delta metadata
        and the Spark catalog, merging comments case-insensitively.
        """
        struct: T.StructType = delta.toDF().schema
        comments_by_lower = self._read_column_comments_lower(full_name_unescaped)
        return self._merge_struct_and_comments(struct, comments_by_lower)

    def _read_column_comments_lower(self, full_name_unescaped: str) -> dict[str, str]:
        """
        Return a map of column name (lower-cased) -> comment.

        Using lower-cased keys avoids casing mismatches between Spark catalog
        and `StructField.name`.
        """
        out: dict[str, str] = {}
        columns = self.spark.catalog.listColumns(full_name_unescaped)
        for col in columns:
            out[col.name.lower()] = col.description or ""
        return out

    def _merge_struct_and_comments(
        self,
        struct: T.StructType,
        comments_by_lower: dict[str, str],
    ) -> list[ColumnState]:
        """Combine the physical schema with catalog comments into `ColumnState` objects."""
        merged: list[ColumnState] = []
        for field in struct.fields:
            merged.append(
                ColumnState(
                    name=field.name,
                    data_type=field.dataType,
                    is_nullable=field.nullable,
                    comment=comments_by_lower.get(field.name.lower(), ""),
                )
            )
        return merged

    def _read_table_comment(self, full_name_unescaped: str) -> str:
        """
        Return the table comment or an empty string.

        Swallows metadata/permission errors and treats them as no comment.
        """
        try:
            return self.spark.catalog.getTable(full_name_unescaped).description or ""
        except Exception:
            # Metadata permissions or missing description → empty string
            return ""

    def _read_table_properties(self, delta: DeltaTable) -> dict[str, str]:
        """
        Return Delta table properties as a plain dict[str, str].

        Reads the `configuration` field from `DeltaTable.detail()`. Returns `{}` when
        the field is absent or on any error.
        """
        try:
            detail_df = delta.detail()
            config_col = next((c for c in detail_df.columns if c.lower() == "configuration"), None)
            if not config_col:
                return {}
            row = detail_df.select(config_col).first()
            if not row:
                return {}
            config_map = row[config_col] or {}
            # Force to plain str->str dict
            return {str(k): str(v) for k, v in dict(config_map).items()}
        except Exception:
            return {}

    def _read_primary_key_state(self, three_part_name: ThreePartTableName) -> PrimaryKeyState | None:
        """
        Return the primary-key state (name + ordered columns), or None if no PK exists.

        Queries `information_schema.table_constraints` and `key_column_usage`.
        """
        name = self._read_primary_key_name_for_table(three_part_name)
        if not name:
            return None
        cols = self._read_primary_key_columns_for_table(three_part_name)
        return PrimaryKeyState(name=name, columns=tuple(cols))

    def _read_primary_key_name_for_table(self, three_part_name: ThreePartTableName) -> str | None:
        """Return the PK constraint name for the table, or None if not present."""
        sql = sql_select_primary_key_name_for_table(three_part_name)
        return self._take_first_value(sql, "name")

    def _read_primary_key_columns_for_table(self, three_part_name: ThreePartTableName) -> list[str]:
        """
        Return the list of PK column names ordered by `ordinal_position`.

        The SQL orders by position; a defensive sort is applied before extraction.
        """
        sql = sql_select_primary_key_columns_for_table(three_part_name)
        rows = self._run(sql)
        # SQL already orders, but keep a defensive sort
        return [r["column_name"] for r in sorted(rows, key=lambda r: r["ordinal_position"])]

    def _run(self, sql: str) -> list[Row]:
        """Execute SQL and collect all rows (use sparingly for small metadata queries)."""
        return self.spark.sql(sql).collect()

    def _take_first_value(self, sql: str, column: str) -> str | None:
        """
        Execute SQL and return the first row's `column` value, or None if no rows.

        Useful for single-value metadata lookups.
        """
        rows = self.spark.sql(sql).take(1)
        return rows[0][column] if rows else None
