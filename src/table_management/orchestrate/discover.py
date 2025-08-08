from __future__ import annotations
import importlib
import pkgutil
from types import ModuleType
from typing import Iterable, List
from src.table_management.models import Table

def discover_tables(package: ModuleType, recurse: bool = True) -> List[Table]:
    """Find all top-level variables that are instances of models.Table."""
    tables: List[Table] = []
    for module in _walk_package(package, recurse=recurse):
        for name in dir(module):
            obj = getattr(module, name, None)
            if isinstance(obj, Table):
                tables.append(obj)
    return tables

def _walk_package(package: ModuleType, recurse: bool) -> Iterable[ModuleType]:
    yield package
    for _, name, is_pkg in pkgutil.iter_modules(package.__path__):
        mod = importlib.import_module(f"{package.__name__}.{name}")
        yield mod
        if is_pkg and recurse:
            yield from _walk_package(mod, recurse=True)
