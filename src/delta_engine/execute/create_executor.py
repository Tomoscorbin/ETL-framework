from typing import Mapping
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

from src.logger import LOGGER
from src.delta_engine.actions import CreateTable
from src.delta_engine.execute.renderer import SqlRenderer
from src.delta_engine.constraints.naming import construct_full_table_name


class CreateExecutor:
    """Executes CreateTable."""

    def __init__(
            self, 
            spark: SparkSession, 
        ) -> None:
        self.spark = spark
        self.renderer = SqlRenderer()

    def apply(self, action: CreateTable) -> None:
        full_name = construct_full_table_name(action.catalog_name, action.schema_name, action.table_name)

        # 1) Create table with schema
        self._create_table(full_name, action.schema_struct, action.table_comment)

        # 2) Properties
        if action.table_properties:
            sql_statement = self.renderer.set_tblproperties(full_name, action.table_properties)
            self._execute(sql_statement)

        # 3) Column comments
        for column_name, comment in (action.column_comments or {}).items():
            sql_statement = self.renderer.set_column_comment(full_name, column_name, comment)
            self._execute(sql_statement)

        LOGGER.info(f"Create: {full_name} — completed")

    # ---- helpers ----

    def _create_table(self, full_name: str, schema: T.StructType, table_comment: str) -> None:
        builder = (
            DeltaTable
            .createIfNotExists(self.spark)
            .tableName(full_name)
            .addColumns(schema)
        )
        if table_comment:
            builder = builder.comment(table_comment)
        builder.execute()

    def _execute(self, sql: str) -> None:
        self.spark.sql(sql)
