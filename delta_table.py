Table
Tomos Corbin<Tomos.Corbin@asda.uk>
​You​
from dataclasses import dataclass, field
from typing import ClassVar

from delta import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import types as T

from cdm.enums import DeltaTableProperty
from cdm.logger import LOGGER
from cdm.models.column import DatabricksColumn
from cdm.models.databricks_object import DatabricksObject
from cdm.utils.sql import escape_sql_string


@dataclass(frozen=True)
class DatabricksTable(DatabricksObject):
    """A table in Databricks."""

    default_delta_properties: ClassVar[dict[str, str]] = {
        DeltaTableProperty.COLUMN_MAPPING_MODE: "name",
    }

    columns: list[DatabricksColumn]
    comment: str = ""
    additional_delta_properties: dict[str, str] = field(default_factory=dict)

    @property
    def can_be_created(self) -> bool:
        """Indicates if the table can be created."""
        return len(self.columns) > 0

    @property
    def expected_delta_properties(self) -> dict[str, str]:
        """The expected Delta Table properties.
        Additional properties override default properties.
        """
        return {**self.default_delta_properties, **self.additional_delta_properties}

    @property
    def partition_column_names(self) -> list[str]:
        """Names of the columns that the table is partitioned by."""
        return [column.name for column in self.columns if column.is_partition]

    @property
    def primary_key_column_names(self) -> list[str]:
        """Names of primary keys."""
        return [column.name for column in self.columns if column.is_primary_key]

    @property
    def column_names(self) -> list[str]:
        """Names of column names defined in the object."""
        return [column.name for column in self.columns]

    @property
    def source_column_rename_map(self) -> dict[str, str]:
        """For use with `.withColumnsRenamed()` to automatically alias source to target
        columns.
        """
        return {
            column.source_column_name: column.name
            for column in self.columns
            if column.source_column_name is not None
            and column.source_column_name != column.name
        }

    @property
    def schema(self) -> T.StructType:
        """Schema of the table in PySpark format."""
        return T.StructType(fields=[column.struct_field for column in self.columns])

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
        try:
            LOGGER.info(f"Ensuring table `{self.full_name}`")
            self._create_if_not_exists(spark=spark)
            self._set_table_comment(spark=spark)
            self._ensure_delta_properties(spark=spark)
            self._add_missing_columns(spark=spark)
            self._drop_extra_columns(spark=spark)
            self._update_column_nullability(spark=spark)
            self._ensure_primary_keys(spark=spark)
            self._set_column_comments(spark=spark)
            LOGGER.info(f"Successfully ensured table `{self.full_name}`")
        except Exception as e:
            LOGGER.error(f"Failed to ensure table `{self.full_name}`: {e}")
            raise e

    def delete(self, spark: SparkSession, condition: Column | str) -> None:
        """Delete rows from the table that match the condition."""
        table = DeltaTable.forName(sparkSession=spark, tableOrViewName=self.full_name)
        table.delete(condition=condition)

    def overwrite(self, dataframe: DataFrame) -> None:
        """Overwrite the table contents."""
        dataframe.select(self.column_names).write.saveAsTable(
            name=self.full_name,
            format="delta",
            mode="overwrite",
            partitionBy=self.partition_column_names or None,
        )

    def _add_missing_columns(self, spark: SparkSession) -> None:
        columns_to_add = ", ".join(
            [
                f"{column.name} {column.data_type.simpleString()}"
                for column in self._identify_missing_columns(spark=spark)
            ]
        )
        if columns_to_add:
            spark.sql(f"ALTER TABLE {self.full_name} ADD COLUMNS ({columns_to_add});")

    def _check_exists(self, spark: SparkSession) -> bool:
        return spark.catalog.tableExists(self.full_name)

    def _create_if_not_exists(self, spark: SparkSession) -> None:
        if self._check_exists(spark=spark):
            return
        delta_table_builder = DeltaTable.createIfNotExists(sparkSession=spark)
        delta_table_builder.tableName(identifier=self.full_name)
        delta_table_builder.addColumns(cols=self.schema)
        if self.comment:
            delta_table_builder.comment(comment=self.comment)
        if self.partition_column_names:
            delta_table_builder.partitionedBy(*self.partition_column_names)
        delta_table_builder.execute()

    def _drop_extra_columns(self, spark: SparkSession) -> None:
        columns_to_drop = ", ".join(self._identify_extra_column_names(spark=spark))
        if columns_to_drop:
            spark.sql(f"ALTER TABLE {self.full_name} DROP COLUMNS ({columns_to_drop});")

    def _ensure_delta_properties(self, spark: SparkSession) -> None:
        """
        Make sure the properties of the table are compatible with Delta Table e.g.
        `enableChangeDataFeed`.

        Ignores existing properties that are not in `expected_delta_properties`. This is
        because we do not want to remove Databricks-managed or organization-managed
        properties such as `delta.minReaderVersion`.
        """
        existing_properties = self._get_existing_delta_properties(spark=spark)
        properties_to_set = [
            f"'{key}' = '{expected_value}'"
            for key, expected_value in self.expected_delta_properties.items()
            if existing_properties.get(key, "") != expected_value
        ]

        if properties_to_set:
            spark.sql(
                f"ALTER TABLE {self.full_name}"
                f" SET TBLPROPERTIES ({', '.join(properties_to_set)});"
            )

    def _ensure_primary_keys(self, spark: SparkSession) -> None:
        if self.primary_key_column_names == self._get_existing_primary_key_column_names(
            spark=spark
        ):
            LOGGER.debug("Primary keys match, no change required.")
            return
        LOGGER.debug(f"Dropping primary keys from `{self.full_name}`.")
        spark.sql(f"ALTER TABLE {self.full_name} DROP PRIMARY KEY IF EXISTS;")
        if not self.primary_key_column_names:
            return
        primary_key_name = (
            f"pk_{self.catalog_name}_{self.schema_name}_{self.object_name}"
        )
        LOGGER.debug(f"Adding primary keys to `{self.full_name}`.")
        spark.sql(
            f"ALTER TABLE {self.full_name}"
            f" ADD CONSTRAINT {primary_key_name}"
            f" PRIMARY KEY ({', '.join(self.primary_key_column_names)});"
        )

    def _get_column(self, column_name: str) -> DatabricksColumn:
        for column in self.columns:
            if column.name == column_name:
                return column
        raise KeyError(f"Column not found: `{column_name}`")

    def _get_existing_delta_properties(self, spark: SparkSession) -> dict[str, str]:
        """
        Query the existing table for it's properties. The query results are returned as
        key-value pairs e.g. `{("delta.enableChangeDataFeed", "true")}`.
        """
        return {
            row["key"]: row["value"]
            for row in spark.sql(f"SHOW TBLPROPERTIES {self.full_name};").collect()
        }

    def _get_existing_primary_key_column_names(self, spark: SparkSession) -> list[str]:
        rows = spark.sql(
            f"""
            SELECT key_column_usage.column_name
            FROM   {self.catalog_name}.INFORMATION_SCHEMA.key_column_usage
            WHERE  EXISTS (
                     SELECT *
                     FROM   {self.catalog_name}.INFORMATION_SCHEMA.table_constraints
                     WHERE  table_constraints.table_catalog = '{self.catalog_name}'
                     AND    table_constraints.table_schema = '{self.schema_name}'
                     AND    table_constraints.table_name = '{self.object_name}'
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

    def _get_existing_schema(self, spark: SparkSession) -> T.StructType:
        return spark.read.table(self.full_name).schema

    def _get_existing_table_comment(self, spark: SparkSession) -> str | None:
        return spark.catalog.getTable(self.full_name).description

    def _identify_extra_column_names(self, spark: SparkSession) -> list[str]:
        """
        Compare the list of desired/expected columns with the list of existing columns
        and return a list of those which are in the existing table but are no longer
        part of our model i.e. columns that are in the existing table but should
        dropped.
        """
        existing_column_names = set(self._get_existing_schema(spark=spark).fieldNames())
        expected_column_names = {column.name for column in self.columns}
        return list(existing_column_names - expected_column_names)

    def _identify_missing_columns(self, spark: SparkSession) -> list[DatabricksColumn]:
        """
        Compare the list of desired/expected columns with the list of existing columns
        and return a list of those which don't yet exist i.e. those that are missing
        from the existing table and should be created.
        """
        existing_column_names = set(self._get_existing_schema(spark=spark).fieldNames())
        return [
            column
            for column in self.columns
            if column.name not in existing_column_names
        ]

    def _identify_columns_with_nullability_changes(
        self, spark: SparkSession
    ) -> list[DatabricksColumn]:
        columns = []
        for existing_column in self._get_existing_schema(spark=spark):
            expected_column = self._get_column(existing_column.name)
            if existing_column.nullable != expected_column.is_nullable:
                columns.append(expected_column)
        return columns

    def _set_column_comment(
        self, column_name: str, comment: str, spark: SparkSession
    ) -> None:
        spark.sql(
            f"ALTER TABLE {self.full_name}"
            f" CHANGE COLUMN {column_name}"
            f" COMMENT '{escape_sql_string(comment)}';"
        )

    def _set_column_comments(self, spark: SparkSession) -> None:
        """
        Checks if the existing column comments match the `DatabricksTable` instance's
        and updates if there is a difference.

        Note:
        - Databricks treats no comment the same as setting the comment to be an
        empty string.
        - Column comments are set to directly mimic those of the `DatabricksTable`
        instance. This means that comments can be _cleared_.
        """
        existing_column_comments = {
            field.name: field.metadata.get("comment", "")
            for field in self._get_existing_schema(spark=spark)
        }
        for column in self.columns:
            if column.name not in existing_column_comments:
                continue
            expected_comment = column.comment or ""
            if expected_comment != existing_column_comments[column.name]:
                self._set_column_comment(
                    column_name=column.name, comment=expected_comment, spark=spark
                )

    def _set_table_comment(self, spark: SparkSession) -> None:
        """
        Checks if the existing table comment matches the `DatabricksTable` instance's
        and updates if there is a difference.

        Note: Table comments are set to directly mimic those of the `DatabricksTable`
        instance. This means that comments can be _cleared_.
        """
        existing_table_comment = self._get_existing_table_comment(spark=spark) or ""
        expected_comment = self.comment or ""
        if existing_table_comment != expected_comment:
            spark.sql(
                f"COMMENT ON TABLE {self.full_name}"
                f" IS '{escape_sql_string(expected_comment)}';"
            )

    def _update_column_nullability(self, spark: SparkSession) -> None:
        columns = self._identify_columns_with_nullability_changes(spark=spark)
        for column in columns:
            operation = "DROP" if column.is_nullable else "SET"
            spark.sql(
                f"ALTER TABLE {self.full_name}"
                f" ALTER COLUMN {column.name} {operation} NOT NULL;"
            )



This e-mail (including any attachments) is private and confidential and may contain privileged material. If you have received this e-mail in error, please notify the sender and delete it (including any attachments) immediately. You must not copy, distribute, disclose or use any of the information in it or any attachments. Telephone calls may be monitored or recorded.
Data classification: Asda Internal