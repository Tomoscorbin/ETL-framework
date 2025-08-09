from pyspark.sql import SparkSession

from src.logger import LOGGER
from src.delta_engine.actions import AlignTable
from src.delta_engine.execute.renderer import SqlRenderer
from src.delta_engine.constraints.naming import construct_full_table_name

class AlignExecutor:
    """Executes AlignTable."""

    def __init__(
            self, 
            spark: SparkSession, 
        ) -> None:
        self.spark = spark
        self.renderer = SqlRenderer()

    def apply(self, action: AlignTable) -> None:
        full_name = construct_full_table_name(action.catalog_name, action.schema_name, action.table_name)

        for addition in action.add_columns:
            sql_statement = self.renderer.add_column(
                    full_name, 
                    addition.name, 
                    addition.data_type, 
                    addition.comment
                )
            self._execute(sql_statement)
            LOGGER.info(f"Added column `{addition.name}`")

        if action.drop_columns:
            names = [c.name for c in action.drop_columns]
            self._execute(self.renderer.drop_columns(full_name, names))  
            LOGGER.info("Dropped columns: `%s`", ", ".join(names))          

        for change in action.change_nullability:
            sql_statement = self.renderer.change_nullability(full_name, change.name, change.make_nullable)
            self._execute(sql_statement)
            LOGGER.info(f"Nullability of column `{change.name}` change to to nullable={change.make_nullable}")

        if action.set_column_comments:
            for column_name, comment in action.set_column_comments.comments.items():
                sql_statement = self.renderer.set_column_comment(full_name, column_name, comment)
                self._execute(sql_statement)
                LOGGER.info(f"Updated comment on column `{column_name}`")

        if action.set_table_comment:
            sql_statement = self.renderer.set_table_comment(full_name, action.set_table_comment.comment)
            self._execute(sql_statement)
            LOGGER.info("Updated table comment")

        if action.set_table_properties:
            sql_statement = self.renderer.set_tblproperties(full_name, action.set_table_properties.properties)
            self._execute(sql_statement)
            LOGGER.info(f"Updated table properties to: `{action.set_table_properties.properties}`")

        LOGGER.info(f"Align: {full_name} — completed")

    def _execute(self, sql: str) -> None:
        self.spark.sql(sql)
