from __future__ import annotations

from typing import Iterable, Mapping, Tuple
import pyspark.sql.types as T
from pyspark.sql import SparkSession

from src.delta_engine.identifiers import FullyQualifiedTableName, fully_qualified_name_to_string
from src.delta_engine.models import Column
from src.delta_engine.utils import quote_identifier, escape_sql_literal, format_tblproperties


class DDLExecutor:
    """
    Façade over Spark SQL for Delta DDL.

    All methods accept a FullyQualifiedTableName. SQL rendering and execution are
    centralized here so the rest of the engine never assembles names ad hoc.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    # ---------- helpers ----------

    @staticmethod
    def _render_column_definition_from_parts(
        column_name: str,
        data_type: T.DataType,
        is_nullable: bool,
        comment: str | None,
    ) -> str:
        """Render: `name` TYPE [NOT NULL] [COMMENT '...']"""
        null_sql = "NOT NULL" if not is_nullable else ""
        comment_sql = f"COMMENT '{escape_sql_literal(comment)}'" if comment else ""
        parts = [quote_identifier(column_name), data_type.simpleString(), null_sql, comment_sql]
        return " ".join(p for p in parts if p)

    @staticmethod
    def _render_column_definition(col: Column) -> str:
        return DDLExecutor._render_column_definition_from_parts(
            column_name=col.name,
            data_type=col.data_type,
            is_nullable=col.is_nullable,
            comment=col.comment or None,
        )

    def _run(self, sql_text: str) -> None:
        # Force execution so DDL errors surface immediately (not lazily later).
        self.spark.sql(sql_text).collect()

    # ---------- create ----------

    def create_table_if_not_exists(
        self,
        full_table_name: FullyQualifiedTableName,
        columns: Tuple[Column, ...],
        table_comment: str | None,
    ) -> None:
        """
        CREATE TABLE IF NOT EXISTS ... USING DELTA [COMMENT '...'].
        Column comments are inlined here for convenience; re-setting later is safe/idempotent.
        """
        full = fully_qualified_name_to_string(full_table_name)

        column_defs = [self._render_column_definition(c) for c in columns]
        columns_sql = ", ".join(column_defs) if column_defs else ""

        parts = []
        if columns_sql:
            parts.append(f"CREATE TABLE IF NOT EXISTS {full} ({columns_sql})")
        else:
            parts.append(f"CREATE TABLE IF NOT EXISTS {full}")
        parts.append("USING DELTA")
        if table_comment is not None:
            parts.append(f"COMMENT '{escape_sql_literal(table_comment)}'")
        sql = " ".join(parts)

        self._run(sql)

    # ---------- properties & comments ----------

    def set_table_properties(self, full_table_name: FullyQualifiedTableName, properties: Mapping[str, str]) -> None:
        if not properties:
            return
        full = fully_qualified_name_to_string(full_table_name)
        sql = f"ALTER TABLE {full} SET TBLPROPERTIES ({format_tblproperties(properties)})"
        self._run(sql)

    def set_table_comment(self, full_table_name: FullyQualifiedTableName, comment: str | None) -> None:
        full = fully_qualified_name_to_string(full_table_name)
        text = "" if comment is None else escape_sql_literal(comment)
        sql = f"COMMENT ON TABLE {full} IS '{text}'"
        self._run(sql)

    def set_column_comment(self, full_table_name: FullyQualifiedTableName, column_name: str, comment: str | None) -> None:
        full = fully_qualified_name_to_string(full_table_name)
        text = "" if comment is None else escape_sql_literal(comment)
        sql = f"COMMENT ON COLUMN {full}.{quote_identifier(column_name)} IS '{text}'"
        self._run(sql)

    # ---------- schema alignment ----------

    def add_column(
        self,
        full_table_name: FullyQualifiedTableName,
        column_name: str,
        data_type: T.DataType,
        is_nullable: bool,
        comment: str | None,
    ) -> None:
        full = fully_qualified_name_to_string(full_table_name)
        col_sql = self._render_column_definition_from_parts(column_name, data_type, is_nullable, comment)
        sql = f"ALTER TABLE {full} ADD COLUMNS ({col_sql})"
        self._run(sql)

    def drop_columns(self, full_table_name: FullyQualifiedTableName, column_names: Iterable[str]) -> None:
        column_list = list(column_names)
        if not column_list:
            return
        full = fully_qualified_name_to_string(full_table_name)
        columns_sql = ", ".join(quote_identifier(c) for c in column_list)
        sql = f"ALTER TABLE {full} DROP COLUMNS ({columns_sql})"
        self._run(sql)

    def set_column_nullability(self, full_table_name: FullyQualifiedTableName, column_name: str, make_nullable: bool) -> None:
        full = fully_qualified_name_to_string(full_table_name)
        if make_nullable:
            sql = f"ALTER TABLE {full} ALTER COLUMN {quote_identifier(column_name)} DROP NOT NULL"
        else:
            sql = f"ALTER TABLE {full} ALTER COLUMN {quote_identifier(column_name)} SET NOT NULL"
        self._run(sql)

    # ---------- constraints ----------

    def add_primary_key(self, full_table_name: FullyQualifiedTableName, constraint_name: str, column_names: Tuple[str, ...]) -> None:
        full = fully_qualified_name_to_string(full_table_name)
        cols_sql = ", ".join(quote_identifier(c) for c in column_names)
        sql = f"ALTER TABLE {full} ADD CONSTRAINT {quote_identifier(constraint_name)} PRIMARY KEY ({cols_sql})"
        self._run(sql)

    def drop_primary_key(self, full_table_name: FullyQualifiedTableName, constraint_name: str) -> None:
        full = fully_qualified_name_to_string(full_table_name)
        sql = f"ALTER TABLE {full} DROP CONSTRAINT {quote_identifier(constraint_name)}"
        self._run(sql)
