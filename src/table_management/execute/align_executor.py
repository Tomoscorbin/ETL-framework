from pyspark.sql import SparkSession

from src.logger import LOGGER
from src.table_management.actions import AlignTable
from src.table_management.execute.renderer import SqlRenderer, construct_full_name

class AlignExecutor:
    """Executes AlignTable."""

    def __init__(
            self, 
            spark: SparkSession, 
        ) -> None:
        self.spark = spark
        self.renderer = SqlRenderer()

    def apply(self, action: AlignTable) -> None:
        full_name = construct_full_name(action.catalog_name, action.schema_name, action.table_name)

        if action.drop_primary_key:
            LOGGER.info("Dropping primary key on table %s", full_name)
            sql_statement = self.renderer.drop_primary_key(full_name)
            self._execute(sql_statement)

        for addition in action.add_columns:
            LOGGER.info(f"Adding column {addition.name}")
            sql_statement = self.renderer.add_column(
                    full_name, 
                    addition.name, 
                    addition.data_type, 
                    addition.is_nullable, 
                    addition.comment
                )
            self._execute(sql_statement)

        for change in action.change_nullability:
            LOGGER.info(f"Changing nullability of column {change.name} to nullable={change.make_nullable}")
            sql_statement = self.renderer.change_nullability(full_name, change.name, change.make_nullable)
            self._execute(sql_statement)

        if action.set_column_comments is not None:
            LOGGER.info(f"Updating comment on column {column_name}")
            for column_name, comment in action.set_column_comments.comments.items():
                sql_statement = self.renderer.set_column_comment(full_name, column_name, comment)
                self._execute(sql_statement)

        if action.set_table_comment is not None:
            LOGGER.info("Updating table comment")
            sql_statement = self.renderer.set_table_comment(full_name, action.set_table_comment.comment)
            self._execute(sql_statement)

        if action.set_table_properties is not None:
            LOGGER.info(f"Updating table properties to: {action.set_table_properties.properties}")
            sql_statement = self.renderer.set_tblproperties(full_name, action.set_table_properties.properties)
            self._execute(sql_statement)

        if action.set_primary_key is not None:
            LOGGER.info(
                f"Adding primary key (%s)",
                ", ".join(action.set_primary_key.columns),
            )
            sql_statement = self.renderer.add_primary_key(full_name, action.set_primary_key.columns)
            self._execute(sql_statement)

        LOGGER.info(f"Align: {full_name} — completed")

    def _execute(self, sql: str) -> None:
        self.spark.sql(sql)
