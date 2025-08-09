from pyspark.sql import SparkSession
from src.delta_engine.actions import AlignTable
from .ddl import DeltaDDL

class AlignExecutor:
    def __init__(self, spark: SparkSession) -> None:
        self.ddl = DeltaDDL(spark)

    def apply(self, action: AlignTable) -> None:
        full = f"{action.catalog_name}.{action.schema_name}.{action.table_name}"

        # add columns (always nullable at add-time)
        for add in action.add_columns:
            self.ddl.add_column(full, add.name, add.data_type, add.comment)

        # drop columns
        if action.drop_columns:
            self.ddl.drop_columns(full, [d.name for d in action.drop_columns])

        # nullability tweaks
        for ch in action.change_nullability:
            self.ddl.set_column_nullability(full, ch.name, ch.make_nullable)

        # column comments
        if action.set_column_comments:
            for col, cmt in action.set_column_comments.comments.items():
                self.ddl.set_column_comment(full, col, cmt)

        # table comment
        if action.set_table_comment:
            self.ddl.set_table_comment(full, action.set_table_comment.comment)

        # table properties
        if action.set_table_properties:
            self.ddl.set_table_properties(full, action.set_table_properties.properties)
