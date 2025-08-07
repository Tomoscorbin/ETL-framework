"""Helpers for discovering and ensuring Delta tables exist."""

import importlib
import pkgutil
from collections.abc import Iterable
from types import ModuleType

from pyspark.sql import SparkSession

from src.logger import LOGGER
from src.models.table import DeltaTable
from src.models.utils import split_qualified_name


def _find_modules_in_package(package: ModuleType, recurse: bool = False) -> list[ModuleType]:
    modules = []
    for _, name, is_package in pkgutil.iter_modules(path=package.__path__):
        module = importlib.import_module(name=f"{package.__name__}.{name}")
        if is_package and recurse:
            modules.extend(_find_modules_in_package(module, recurse=True))
        else:
            modules.append(module)
    return modules


def _get_all_delta_tables_in_module(
    module: ModuleType, instance_type: type[DeltaTable]
) -> list[DeltaTable]:
    tables = []
    for attr in dir(module):
        obj = getattr(module, attr)
        is_match = isinstance(obj, instance_type)
        if is_match:
            tables.append(obj)
    return tables


def check_primary_key_exists(
    spark: SparkSession,
    table_full_name: str,
    column_name: str,
) -> bool:
    """Check if a given column is defined as a primary key in a specified table."""
    catalog_name, schema_name, table_name = split_qualified_name(table_full_name)
    query = f"""
      SELECT kcu.column_name
      FROM {catalog_name}.information_schema.table_constraints tc
      JOIN {catalog_name}.information_schema.key_column_usage kcu
        ON  tc.constraint_catalog = kcu.constraint_catalog
        AND tc.constraint_schema  = kcu.constraint_schema
        AND tc.constraint_name    = kcu.constraint_name
      WHERE tc.constraint_type  = 'PRIMARY KEY'
        AND tc.table_schema     = '{schema_name}'
        AND tc.table_name       = '{table_name}'
        AND kcu.column_name     = '{column_name}'
    """
    return bool(spark.sql(query).limit(1).collect())


def _get_existing_foreign_key_names(delta_table: DeltaTable, spark: SparkSession) -> list[str]:
    rows = spark.sql(
        f"""
            SELECT constraint_name
            FROM {delta_table.catalog_name}.information_schema.table_constraints
            WHERE table_catalog = '{delta_table.catalog_name}'
            AND table_schema = '{delta_table.schema_name}'
            AND table_name = '{delta_table.table_name}'
            AND constraint_type = 'FOREIGN KEY'
    """
    ).collect()
    return [row["constraint_name"] for row in rows]


def _ensure_foreign_keys(delta_table: DeltaTable, spark: SparkSession) -> None:
    existing_names = set(_get_existing_foreign_key_names(delta_table, spark))
    expected_constraints = delta_table.foreign_key_constraints
    expected_names = {c["constraint_name"] for c in expected_constraints}

    # Drop extra constraints
    for name in existing_names - expected_names:
        spark.sql(f"ALTER TABLE {delta_table.full_name} DROP CONSTRAINT {name};")

    # Add missing constraints
    for c in expected_constraints:
        name = c["constraint_name"]
        reference_table = c["reference_table"]
        reference_column = c["reference_column"]
        reference_primary_key_exists = check_primary_key_exists(
            spark=spark, table_full_name=reference_table, column_name=reference_column
        )  # TODO: make it reference the DeltaTable object and not the UC object

        if name not in existing_names:
            if reference_primary_key_exists:
                spark.sql(f"""
                    ALTER TABLE {delta_table.full_name}
                    ADD CONSTRAINT {name}
                    FOREIGN KEY ({c["source_column"]})
                    REFERENCES {c["reference_table"]}({c["reference_column"]})
                """)
            else:
                LOGGER.warning(
                    f"Skipping FK {name}: referenced primary key"
                    " {reference_table}({reference_column}) does not exist."
                    " This column must be defined as the primary key first."
                )


def _ensure_foreign_keys_exist(tables_to_ensure: Iterable[DeltaTable], spark: SparkSession) -> None:
    tables_with_errors = []
    for delta_table in tables_to_ensure:
        try:
            _ensure_foreign_keys(delta_table, spark)
            LOGGER.info("FK build: %s ✓", delta_table.full_name)
        except Exception as e:
            LOGGER.error("FK build: %s ✗ (%s)", delta_table.full_name, e)
            tables_with_errors.append(delta_table.full_name)

    if tables_with_errors:
        raise RuntimeError(f"Failed to ensure foreign keys on: {tables_with_errors}")


def _ensure_delta_tables_exists(
    tables_to_create: Iterable[DeltaTable], spark: SparkSession
) -> None:
    tables_with_exceptions = []
    for delta_table in tables_to_create:
        try:
            delta_table.ensure(spark)
            LOGGER.info("DDL build: %s ✓", delta_table.full_name)
        except Exception as e:
            LOGGER.error("DDL build: %s ✗ (%s)", delta_table.full_name, e)
            tables_with_exceptions.append(delta_table.full_name)

    if tables_with_exceptions:
        raise RuntimeError(f"Failed to ensure tables: {tables_with_exceptions}")


def ensure_all_delta_tables(package: ModuleType, spark: SparkSession) -> None:
    """Ensure all delta tables exist in Unity Catalog."""
    tables_to_ensure = []
    for module in _find_modules_in_package(package=package, recurse=True):
        delta_tables = _get_all_delta_tables_in_module(module=module, instance_type=DeltaTable)
        tables_to_ensure.extend(delta_tables)

    _ensure_delta_tables_exists(tables_to_create=tables_to_ensure, spark=spark)
    _ensure_foreign_keys_exist(tables_to_ensure=tables_to_ensure, spark=spark)
