"""
DDL Executor

Façade over Spark SQL for Delta DDL.

- All methods accept a FullyQualifiedTableName.
- This module is the single place that renders SQL for identifiers.
- Identifiers are ALWAYS quoted using `quote_fully_qualified_table_name(...)`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

import pyspark.sql.types as T
from pyspark.sql import SparkSession

from src.delta_engine.identifiers import FullyQualifiedTableName, quote_fully_qualified_table_name
from src.delta_engine.models import Column
from src.delta_engine.utils import escape_sql_literal, format_tblproperties, quote_identifier


class DDLExecutor:
    """
    Façade over Spark SQL for Delta DDL.

    All methods accept a FullyQualifiedTableName. SQL rendering and execution are
    centralised here so the rest of the engine never assembles names ad hoc.
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
        escaped_comment = None if comment is None or comment == "" else escape_sql_literal(comment)
        comment_sql = f"COMMENT '{escaped_comment}'" if escaped_comment is not None else ""
        parts = [quote_identifier(column_name), data_type.simpleString(), null_sql, comment_sql]
        return " ".join(p for p in parts if p)

    @staticmethod
    def _render_column_definition(column: Column) -> str:
        return DDLExecutor._render_column_definition_from_parts(
            column_name=column.name,
            data_type=column.data_type,
            is_nullable=column.is_nullable,
            comment=column.comment or None,
        )

    def _run(self, sql_text: str) -> None:
        """Execute the SQL immediately so DDL errors surface now (not lazily)."""
        self.spark.sql(sql_text).collect()

    @staticmethod
    def _quoted(full_table_name: FullyQualifiedTableName) -> str:
        """Backticked full identifier: `` `catalog`.`schema`.`table` ``."""
        return quote_fully_qualified_table_name(full_table_name)

    # ---------- create ----------

    def create_table_if_not_exists(
        self,
        full_table_name: FullyQualifiedTableName,
        columns: tuple[Column, ...],
        table_comment: str | None,
    ) -> None:
        """
        CREATE TABLE IF NOT EXISTS ... USING DELTA [COMMENT '...'].

        Column comments are inlined for convenience; re-setting later is safe/idempotent.
        """
        quoted = self._quoted(full_table_name)

        column_definitions = [self._render_column_definition(column) for column in columns]
        columns_sql = ", ".join(column_definitions) if column_definitions else ""

        parts: list[str] = []
        if columns_sql:
            parts.append(f"CREATE TABLE IF NOT EXISTS {quoted} ({columns_sql})")
        else:
            parts.append(f"CREATE TABLE IF NOT EXISTS {quoted}")
        parts.append("USING DELTA")
        if table_comment is not None:
            escaped_comment = escape_sql_literal(table_comment)
            parts.append(f"COMMENT '{escaped_comment}'")
        sql = " ".join(parts)

        self._run(sql)

    # ---------- properties & comments ----------

    def set_table_properties(
        self,
        full_table_name: FullyQualifiedTableName,
        properties: Mapping[str, str],
    ) -> None:
        if not properties:
            return
        quoted = self._quoted(full_table_name)
        props_sql = format_tblproperties(properties)
        sql = f"ALTER TABLE {quoted} SET TBLPROPERTIES ({props_sql})"
        self._run(sql)

    def set_table_comment(
        self,
        full_table_name: FullyQualifiedTableName,
        comment: str | None,
    ) -> None:
        quoted = self._quoted(full_table_name)
        text = "" if comment is None else escape_sql_literal(comment)
        sql = f"COMMENT ON TABLE {quoted} IS '{text}'"
        self._run(sql)

    def set_column_comment(
        self,
        full_table_name: FullyQualifiedTableName,
        column_name: str,
        comment: str | None,
    ) -> None:
        quoted = self._quoted(full_table_name)
        text = "" if comment is None else escape_sql_literal(comment)
        sql = f"COMMENT ON COLUMN {quoted}.{quote_identifier(column_name)} IS '{text}'"
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
        quoted = self._quoted(full_table_name)
        column_sql = self._render_column_definition_from_parts(
            column_name=column_name,
            data_type=data_type,
            is_nullable=is_nullable,
            comment=comment,
        )
        sql = f"ALTER TABLE {quoted} ADD COLUMNS ({column_sql})"
        self._run(sql)

    def drop_columns(
        self,
        full_table_name: FullyQualifiedTableName,
        column_names: Iterable[str],
    ) -> None:
        column_list = list(column_names)
        if not column_list:
            return
        quoted = self._quoted(full_table_name)
        columns_sql = ", ".join(quote_identifier(name) for name in column_list)
        sql = f"ALTER TABLE {quoted} DROP COLUMNS ({columns_sql})"
        self._run(sql)

    def set_column_nullability(
        self,
        full_table_name: FullyQualifiedTableName,
        column_name: str,
        make_nullable: bool,
    ) -> None:
        quoted = self._quoted(full_table_name)
        column_ident = quote_identifier(column_name)
        if make_nullable:
            sql = f"ALTER TABLE {quoted} ALTER COLUMN {column_ident} DROP NOT NULL"
        else:
            sql = f"ALTER TABLE {quoted} ALTER COLUMN {column_ident} SET NOT NULL"
        self._run(sql)

    # ---------- constraints ----------

    def add_primary_key(
        self,
        full_table_name: FullyQualifiedTableName,
        constraint_name: str,
        column_names: tuple[str, ...],
    ) -> None:
        quoted = self._quoted(full_table_name)
        cols_sql = ", ".join(quote_identifier(c) for c in column_names)
        constraint_ident = quote_identifier(constraint_name)
        sql = f"ALTER TABLE {quoted} ADD CONSTRAINT {constraint_ident} PRIMARY KEY ({cols_sql})"
        self._run(sql)

    def drop_primary_key(
        self,
        full_table_name: FullyQualifiedTableName,
        constraint_name: str,
    ) -> None:
        quoted = self._quoted(full_table_name)
        constraint_ident = quote_identifier(constraint_name)
        sql = f"ALTER TABLE {quoted} DROP CONSTRAINT {constraint_ident}"
        self._run(sql)
