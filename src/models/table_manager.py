from typing import TYPE_CHECKING

import pyspark.sql.types as T
from delta import DeltaTable as dt
from pyspark.sql import SparkSession

from src.models.column import DeltaColumn

if TYPE_CHECKING:
    from src.models.table import DeltaTable


class DeltaTableManager:
    """Delta Table DDL Manager."""

    def __init__(self, delta_table: DeltaTable):
        self.delta_table = delta_table

    def ensure(self, spark: SparkSession) -> None:  # pragma: no cover
        """Ensure that the table:
        - Exists.
        - Has the correct comment.
        - Has the correct delta properties.
        - Has the correct columns.
        - Columns have the correct nullability.
        - Has the correct primary keys.
        - Has the correct column comments.
        """
        self._create_if_not_exists(spark)
        self._set_table_comment(spark)
        self._ensure_delta_properties(spark)
        self._add_missing_columns(spark)
        self._drop_extra_columns(spark)
        self._update_column_nullability(spark)
        self._ensure_primary_keys(spark)
        self._set_column_comments(spark=spark)

    def _get_column(self, column_name: str) -> DeltaColumn:
        for column in self.delta_table.columns:
            if column.name == column_name:
                return column
        raise KeyError(f"Column not found: `{column_name}`")

    def _get_existing_schema(self, spark: SparkSession) -> T.StructType:
        return spark.read.table(self.delta_table.full_name).schema

    def _get_existing_column_names(self, spark: SparkSession) -> set[str]:
        return set(self._get_existing_schema(spark).fieldNames())

    def _get_existing_column_comments(self, spark: SparkSession) -> dict[str, str]:
        return {field.name: field.metadata.get("comment", "") for field in self._get_existing_schema(spark)}

    def _get_existing_primary_key_column_names(self, spark: SparkSession) -> list[str]:
        rows = spark.sql(
            f"""
            SELECT key_column_usage.column_name
            FROM   {self.delta_table.catalog_name}.INFORMATION_SCHEMA.key_column_usage
            WHERE  EXISTS (
                     SELECT *
                     FROM   {self.delta_table.catalog_name}.INFORMATION_SCHEMA.table_constraints
                     WHERE  table_constraints.table_catalog = '{self.delta_table.catalog_name}'
                     AND    table_constraints.table_schema = '{self.delta_table.schema_name}'
                     AND    table_constraints.table_name = '{self.delta_table.table_name}'
                     AND    table_constraints.constraint_catalog = key_column_usage.constraint_catalog
                     AND    table_constraints.constraint_schema = key_column_usage.constraint_schema
                     AND    table_constraints.constraint_name = key_column_usage.constraint_name
                   )
            ORDER
                BY key_column_usage.ordinal_position
            ;
            """  # noqa: E501
        ).collect()
        return [row["column_name"] for row in rows]

    def _identify_missing_columns(self, spark: SparkSession) -> list[DeltaColumn]:
        existing_column_names = self._get_existing_column_names(spark)
        return [column for column in self.delta_table.columns if column.name not in existing_column_names]

    def _identify_extra_column_names(self, spark: SparkSession) -> list[str]:
        existing_column_names = self._get_existing_column_names(spark)
        expected_column_names = {column.name for column in self.delta_table.columns}
        return list(existing_column_names - expected_column_names)

    def _identify_columns_with_nullability_changes(self, spark: SparkSession) -> list[DeltaColumn]:
        columns = []
        for existing_column in self._get_existing_schema(spark):
            expected_column = self._get_column(existing_column.name)
            if existing_column.nullable != expected_column.is_nullable:
                columns.append(expected_column)
        return columns

    def _create_if_not_exists(self, spark: SparkSession) -> None:
        if self.delta_table.check_exists(spark):
            return

        delta_table_builder = dt.createIfNotExists(spark)
        delta_table_builder.tableName(identifier=self.delta_table.full_name)
        delta_table_builder.addColumns(cols=self.delta_table.schema)

        if self.delta_table.comment:
            delta_table_builder.comment(comment=self.delta_table.comment)

        delta_table_builder.execute()

    def _set_table_comment(self, spark: SparkSession) -> None:
        existing_comment = spark.catalog.getTable(self.delta_table.full_name).description
        expected_comment = self.delta_table.comment

        if existing_comment != expected_comment:
            spark.sql(f"COMMENT ON TABLE {self.delta_table.full_name} IS '{expected_comment}';")

    def _set_column_comment(self, column_name: str, comment: str, spark: SparkSession) -> None:
        spark.sql(f"ALTER TABLE {self.delta_table.full_name} CHANGE COLUMN {column_name} COMMENT '{comment}';")

    def _set_column_comments(self, spark: SparkSession) -> None:
        existing_column_comments = self._get_existing_column_comments(spark)
        for column in self.delta_table.columns:
            if column.name not in existing_column_comments:
                continue
            expected_comment = column.comment or ""
            if expected_comment != existing_column_comments[column.name]:
                self._set_column_comment(column_name=column.name, comment=expected_comment, spark=spark)

    def _ensure_delta_properties(self, spark: SparkSession) -> None:
        existing_properties = {
            row["key"]: row["value"] for row in spark.sql(f"SHOW TBLPROPERTIES {self.delta_table.full_name};").collect()
        }

        properties_to_set = [
            f"'{key}' = '{expected_value}'"
            for key, expected_value in self.delta_table.expected_delta_properties.items()
            if existing_properties.get(key, "") != expected_value
        ]

        if properties_to_set:
            spark.sql(f"ALTER TABLE {self.delta_table.full_name} SET TBLPROPERTIES ({', '.join(properties_to_set)});")

    def _add_missing_columns(self, spark: SparkSession) -> None:
        missing_columns = self._identify_missing_columns(spark)
        columns_to_add = ", ".join([f"{column.name} {column.data_type.simpleString()}" for column in missing_columns])
        if columns_to_add:
            spark.sql(f"ALTER TABLE {self.delta_table.full_name} ADD COLUMNS ({columns_to_add});")

    def _drop_extra_columns(self, spark: SparkSession) -> None:
        columns_to_drop = ", ".join(self._identify_extra_column_names(spark))
        if columns_to_drop:
            spark.sql(f"ALTER TABLE {self.delta_table.full_name} DROP COLUMNS ({columns_to_drop});")

    def _update_column_nullability(self, spark: SparkSession) -> None:
        columns = self._identify_columns_with_nullability_changes(spark)
        for column in columns:
            operation = "DROP" if column.is_nullable else "SET"
            spark.sql(f"ALTER TABLE {self.delta_table.full_name} ALTER COLUMN {column.name} {operation} NOT NULL;")

    def _ensure_primary_keys(self, spark: SparkSession) -> None:
        if self.delta_table.primary_key_column_names == self._get_existing_primary_key_column_names(spark):
            return

        spark.sql(f"ALTER TABLE {self.delta_table.full_name} DROP PRIMARY KEY IF EXISTS;")

        if not self.delta_table.primary_key_column_names:
            return

        primary_key_name = (
            f"pk_{self.delta_table.catalog_name}_{self.delta_table.schema_name}_{self.delta_table.table_name}"
        )
        spark.sql(
            f"ALTER TABLE {self.delta_table.full_name}"
            f" ADD CONSTRAINT {primary_key_name}"
            f" PRIMARY KEY ({', '.join(self.delta_table.primary_key_column_names)});"
        )
