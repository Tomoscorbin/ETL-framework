from typing import Mapping, Iterable
from pyspark.sql import SparkSession
import pyspark.sql.types as T

class DeltaDDL:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    # --- creation via Delta API
    def create_table_if_not_exists(
        self,
        full_name: str,
        schema: T.StructType,
        table_comment: str = "",
    ) -> None:
        from delta.tables import DeltaTable
        builder = (
            DeltaTable
            .createIfNotExists(self.spark)
            .tableName(full_name)
            .addColumns(schema)
        )
        if table_comment:
            builder = builder.comment(table_comment)
        builder.execute()

    def set_table_properties(self, full_name: str, props: Mapping[str, str]) -> None:
        if not props:
            return
        assignments = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])
        self.spark.sql(f"ALTER TABLE {full_name} SET TBLPROPERTIES ({assignments})")

    def add_column(self, full_name: str, name: str, dtype: T.DataType, comment: str = "") -> None:
        # UC/Delta forbids NOT NULL in ADD COLUMNS; nullability change must be a separate ALTER
        dtype_sql = dtype.simpleString()
        comment_sql = f" COMMENT '{comment}'" if comment else ""
        self.spark.sql(f"ALTER TABLE {full_name} ADD COLUMNS ({name} {dtype_sql}{comment_sql})")

    def drop_columns(self, full_name: str, names: Iterable[str]) -> None:
        cols = ", ".join(names)
        self.spark.sql(f"ALTER TABLE {full_name} DROP COLUMNS ({cols})")

    def set_column_nullability(self, full_name: str, name: str, make_nullable: bool) -> None:
        op = "DROP NOT NULL" if make_nullable else "SET NOT NULL"
        self.spark.sql(f"ALTER TABLE {full_name} ALTER COLUMN {name} {op}")

    def set_column_comment(self, full_name: str, name: str, comment: str) -> None:
        self.spark.sql(
            f"ALTER TABLE {full_name} CHANGE COLUMN {name} COMMENT '{comment or ''}'"
        )

    def set_table_comment(self, full_name: str, comment: str) -> None:
        self.spark.sql(f"COMMENT ON TABLE {full_name} IS '{comment or ''}'")
