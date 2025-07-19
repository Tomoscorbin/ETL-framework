import importlib
import pkgutil
from pyspark.sql import SparkSession
from typing import Iterable
from types import ModuleType

from src.models.table import DeltaTable


def _find_modules_in_package(
    package: ModuleType, recurse: bool = False
) -> list[ModuleType]:
    modules = []
    for _, name, is_package in pkgutil.iter_modules(path=package.__path__):
        module = importlib.import_module(name=f"{package.__name__}.{name}")
        if is_package and recurse:
            modules.extend(_find_modules_in_package(module, recurse=True))
        else:
            modules.append(module)
    return modules

def _get_all_delta_tables_in_module(module: ModuleType, instance_type: type[DeltaTable]) -> list[DeltaTable]:
    return [
        getattr(module, attribute)
        for attribute in dir(module)
        if isinstance(getattr(module, attribute), instance_type)
    ]

def _ensure_delta_tables_exists(
    tables_to_create: Iterable[DeltaTable],
    spark: SparkSession
) -> None:
    tables_with_exceptions = []
    for delta_table in tables_to_create:
        try:
            delta_table.ensure(spark)
        except Exception as e:
            tables_with_exceptions.append((delta_table, e))
    
    if tables_with_exceptions:
        raise RuntimeError(f"Failed to ensure tables: {tables_with_exceptions}")

def _create_schema(schema_name: str, catalog_name: str, spark: SparkSession) -> None:
    qualified_schema_name = f"{catalog_name}.{schema_name}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema_name}")

def _ensure_schema(schema_name: str, catalog_name: str, spark: SparkSession) -> None:
    _create_schema(schema_name=schema_name, catalog_name=catalog_name, spark=spark)
    # TODO: set permissions on schemas

def ensure_all_schemas(schema_names: Iterable[str], catalog_name: str, spark: SparkSession) -> None:
    for schema_name in schema_names:
        _ensure_schema(schema_name=schema_name, catalog_name=catalog_name, spark=spark)

def ensure_all_delta_tables(package: ModuleType, spark: SparkSession) -> None:
    tables_to_create = []
    for module in _find_modules_in_package(package=package, recurse=True):
        module_objects = _get_all_delta_tables_in_module(module=module, instance_type=DeltaTable)
        tables_to_create.extend(module_objects)

    _ensure_delta_tables_exists(
        tables_to_create=tables_to_create,
        spark=spark
    )