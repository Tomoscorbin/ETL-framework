from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from src.delta_engine.state.states import CatalogState

class Aspect(StrEnum):
    SCHEMA     = "schema"
    COMMENTS   = "comments"
    PROPERTIES = "properties"
    PRIMARY_KEY = "primary_key"

class SnapshotPolicy(StrEnum):
    PERMISSIVE = "permissive"
    STRICT = "strict"

@dataclass(frozen=True)
class TableIdentity:
    catalog: str
    schema: str
    table: str

@dataclass(frozen=True)
class SnapshotRequest:
    tables: tuple[TableIdentity, ...]
    aspects: frozenset[Aspect]
    policy: SnapshotPolicy = SnapshotPolicy.PERMISSIVE

@dataclass(frozen=True)
class SnapshotWarning:
    table: TableIdentity | None
    aspect: Aspect | None
    message: str

@dataclass(frozen=True)
class SnapshotResult:
    state: CatalogState
    warnings: tuple[SnapshotWarning, ...]

class CatalogStateReader(Protocol):
    def snapshot(self, request: SnapshotRequest) -> SnapshotResult: ...
