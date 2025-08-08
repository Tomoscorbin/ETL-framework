"""Helpers for discovering and ensuring Delta tables exist."""

import importlib
import pkgutil
from collections.abc import Iterable
from types import ModuleType

from pyspark.sql import SparkSession

from src.logger import LOGGER
from src.models.table import DeltaTable
from src.models.utils import MANAGED_CONSTRAINT_PREFIX, construct_qualified_name


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


def _get_existing_foreign_key_names(delta_table: DeltaTable, spark: SparkSession) -> list[str]:
    rows = spark.sql(
        f"""
        SELECT constraint_name
        FROM {delta_table.catalog_name}.information_schema.table_constraints
        WHERE table_catalog = '{delta_table.catalog_name}'
          AND table_schema  = '{delta_table.schema_name}'
          AND table_name    = '{delta_table.table_name}'
          AND constraint_type = 'FOREIGN KEY'
        """
    ).collect()
    return [r["constraint_name"] for r in rows]


def _add_foreign_key(delta_table: DeltaTable, spark: SparkSession, c: dict[str, str]) -> None:
    sql = f"""
        ALTER TABLE {delta_table.full_name}
        ADD CONSTRAINT {c["constraint_name"]}
        FOREIGN KEY ({c["source_column"]})
        REFERENCES {construct_qualified_name(*c["reference_table"].split(".", 2))}
                   ({c["reference_column"]})
    """
    spark.sql(sql)


def _ensure_foreign_keys(delta_table: DeltaTable, spark: SparkSession) -> None:
    existing = set(_get_existing_foreign_key_names(delta_table, spark))
    managed_existing = {n for n in existing if n.startswith(MANAGED_CONSTRAINT_PREFIX)}

    expected = delta_table.foreign_key_constraints
    expected_names = {c["constraint_name"] for c in expected}

    # Drop only our managed constraints that we no longer expect
    for name in sorted(managed_existing - expected_names):
        sql = f"""
            ALTER TABLE {delta_table.full_name}
            DROP CONSTRAINT {name}
        """
        spark.sql(sql)
        LOGGER.info("Dropped FK %s on %s", name, delta_table.full_name)

    # Add any missing expected constraints
    for c in expected:
        name = c["constraint_name"]
        if name in existing:
            continue
        _add_foreign_key(delta_table, spark, c)
        LOGGER.info("Added FK %s on %s", name, delta_table.full_name)


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
