from pyspark.sql import SparkSession
import pyspark.sql.types as T
from src.delta_engine.actions import CreateTable
from .ddl import DeltaDDL

class CreateExecutor:
    def __init__(self, spark: SparkSession) -> None:
        self.ddl = DeltaDDL(spark)

    def apply(self, action: CreateTable) -> None:
        full = f"{action.catalog_name}.{action.schema_name}.{action.table_name}"

        # 1) Create skeleton with schema + comment
        self.ddl.create_table_if_not_exists(full, action.schema_struct, action.table_comment)

        # 2) Properties (e.g., delta.columnMapping.mode = name)
        self.ddl.set_table_properties(full, action.table_properties)

        # 3) Column comments
        for col, comment in (action.column_comments or {}).items():
            if comment:
                self.ddl.set_column_comment(full, col, comment)
