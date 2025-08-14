from __future__ import annotations

from typing import Iterable, Mapping, Tuple
import pyspark.sql.types as T
from pyspark.sql import SparkSession

from src.delta_engine.identifiers import FullyQualifiedTableName, fully_qualified_name_to_string
from src.delta_engine.utils import quote_identifier, escape_sql_literal, format_tblproperties


class DDLExecutor:
    """
    Façade over Spark SQL for Delta DDL.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    # ---------- helpers ----------

    @staticmethod
    def _render_column_definition(
        column_name: str,
        data_type: T.DataType,
        is_nullable: bool,
        comment: str | None,
    ) -> str:
        null_sql = "NOT NULL" if not is_nullable else ""
        comment_sql = f"COMMENT '{escape_sql_literal(comment)}'" if comment else ""
        parts = [quote_identifier(column_name), data_type.simpleString(), null_sql, comment_sql]
        return " ".join(p for p in parts if p)
    
    def _run(self, sql_text: str) -> None:
        self.spark.sql(sql_text)

    # ---------- create ----------

    def create_table_if_not_exists(
        self,
        name: FullyQualifiedTableName,
        schema_struct: T.StructType,
        table_comment: str | None,
    ) -> None:
        """CREATE TABLE IF NOT EXISTS ... USING DELTA [COMMENT '...'] (no properties here)."""
        full = self.fully_qualified_name_to_string(name)

        column_defs: list[str] = []
        for field in schema_struct.fields:
            # Column comments are set later explicitly.
            column_defs.append(self._render_column_definition(field.name, field.dataType, field.nullable, None))
        columns_sql = ", ".join(column_defs)

        parts = [f"CREATE TABLE IF NOT EXISTS {full} ({columns_sql})", "USING DELTA"]
        if table_comment is not None:
            parts.append(f"COMMENT '{escape_sql_literal(table_comment)}'")
        sql = " ".join(parts)

        self._run(sql)

    # ---------- properties & comments ----------

    def set_table_properties(self, name: FullyQualifiedTableName, properties: Mapping[str, str]) -> None:
        if not properties:
            return
        full = self.fully_qualified_name_to_string(name)
        sql = f"ALTER TABLE {full} SET TBLPROPERTIES ({format_tblproperties(properties)})"
        self._run(sql)

    def set_table_comment(self, name: FullyQualifiedTableName, comment: str | None) -> None:
        full = self.fully_qualified_name_to_string(name)
        text = "" if comment is None else escape_sql_literal(comment)
        sql = f"COMMENT ON TABLE {full} IS '{text}'"
        self._run(sql)

    def set_column_comment(self, name: FullyQualifiedTableName, column: str, comment: str | None) -> None:
        full = self.fully_qualified_name_to_string(name)
        text = "" if comment is None else escape_sql_literal(comment)
        sql = f"COMMENT ON COLUMN {full}.{quote_identifier(column)} IS '{text}'"
        self._run(sql)

    # ---------- schema alignment ----------

    def add_column(
        self,
        name: FullyQualifiedTableName,
        column_name: str,
        data_type: T.DataType,
        is_nullable: bool,
        comment: str | None,
    ) -> None:
        full = self.fully_qualified_name_to_string(name)
        col_sql = self._render_column_definition(column_name, data_type, is_nullable, comment)
        sql = f"ALTER TABLE {full} ADD COLUMNS ({col_sql})"
        self._run(sql)

    def drop_columns(self, name: FullyQualifiedTableName, column_names: Iterable[str]) -> None:
        columns_sql = ", ".join(quote_identifier(c) for c in column_names)
        if not columns_sql:
            return
        full = self.fully_qualified_name_to_string(name)
        sql = f"ALTER TABLE {full} DROP COLUMNS ({columns_sql})"
        self._run(sql)

    def set_column_nullability(self, name: FullyQualifiedTableName, column: str, make_nullable: bool) -> None:
        full = self.fully_qualified_name_to_string(name)
        if make_nullable:
            sql = f"ALTER TABLE {full} ALTER COLUMN {quote_identifier(column)} DROP NOT NULL"
        else:
            sql = f"ALTER TABLE {full} ALTER COLUMN {quote_identifier(column)} SET NOT NULL"
        self._run(sql)

    # ---------- constraints ----------

    def add_primary_key(self, name: FullyQualifiedTableName, constraint_name: str, columns: Tuple[str, ...]) -> None:
        full = self.fully_qualified_name_to_string(name)
        cols_sql = ", ".join(quote_identifier(c) for c in columns)
        sql = f"ALTER TABLE {full} ADD CONSTRAINT {quote_identifier(constraint_name)} PRIMARY KEY ({cols_sql})"
        self._run(sql)

    def drop_primary_key(self, name: FullyQualifiedTableName, constraint_name: str) -> None:
        full = self.fully_qualified_name_to_string(name)
        sql = f"ALTER TABLE {full} DROP CONSTRAINT {quote_identifier(constraint_name)}"
        self._run(sql)
