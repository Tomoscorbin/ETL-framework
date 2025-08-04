import importlib
import pkgutil
from collections.abc import Iterable
from types import ModuleType

from pyspark.sql import SparkSession

from src.logger import LOGGER
from src.models.table import DeltaTable


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
