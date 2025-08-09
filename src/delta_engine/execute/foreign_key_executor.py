from pyspark.sql import SparkSession
from src.logger import LOGGER
from src.delta_engine.actions import ForeignKeyAdd, ForeignKeyDrop
from src.delta_engine.execute.renderer import SqlRenderer, construct_full_name

class ForeignKeyExecutor:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.renderer = SqlRenderer()

    def drop(self, action: ForeignKeyDrop) -> None:
        full = construct_full_name(action.catalog_name, action.schema_name, action.table_name)
        LOGGER.info("Dropping FK %s on %s", action.constraint_name, full)
        self.spark.sql(self.renderer.drop_foreign_key(full, action.constraint_name))

    def add(self, action: ForeignKeyAdd) -> None:
        src_full = construct_full_name(action.catalog_name, action.schema_name, action.source_table_name)
        ref_full = construct_full_name(action.catalog_name, action.schema_name, action.reference_table_name)
        LOGGER.info("Adding FK %s on %s -> %s", action.constraint_name, src_full, ref_full)
        sql = self.renderer.add_foreign_key(
            full_name=src_full,
            constraint_name=action.constraint_name,
            source_columns=action.source_columns,
            reference_full_name=ref_full,
            reference_columns=action.reference_columns,
        )
        self.spark.sql(sql)
