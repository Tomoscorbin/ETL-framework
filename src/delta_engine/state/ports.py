from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, ClassVar

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.state.states import CatalogState

_MAX_WARNING_LENGTH = 300  # single source of truth


class Aspect(StrEnum):
    SCHEMA = "SCHEMA"
    PRIMARY_KEY = "PRIMARY_KEY"
    COMMENTS = "COMMENTS"
    PROPERTIES = "PROPERTIES"

class SnapshotPolicy(StrEnum):
    PERMISSIVE = "permissive"
    STRICT = "strict"

@dataclass(frozen=True)
class SnapshotRequest:
    tables: tuple[FullyQualifiedTableName, ...]
    aspects: frozenset[Aspect]
    policy: SnapshotPolicy = SnapshotPolicy.PERMISSIVE

@dataclass(frozen=True)
class SnapshotResult:
    state: CatalogState
    warnings: tuple[SnapshotWarning, ...]

@dataclass(frozen=True)
class SnapshotWarning:
    """
    A warning produced while snapshotting live catalog state.
    - table: optional identifier
    - aspect: which slice of metadata we were reading
    - message: short, readable text (single line, truncated)
    """
    table: object | None
    aspect: Aspect
    message: str

    # ---- Defaults for each aspect (used when caller doesn't supply a prefix) ----
    _DEFAULT_PREFIX: ClassVar[dict[Aspect, str]] = {
        Aspect.SCHEMA: "Failed to read table schema",
        Aspect.PRIMARY_KEY: "Failed to read primary key metadata",
        Aspect.COMMENTS: "Failed to read column comments",
        Aspect.PROPERTIES: "Failed to read table properties",
    }

    @staticmethod
    def _format_exception_brief(error: object) -> str:
        if isinstance(error, BaseException):
            name = type(error).__name__
            text = str(error).strip()
            first = text.splitlines()[0] if text else ""
            brief = f"{name}: {first}" if first else name
        else:
            text = str(error).strip()
            brief = text.splitlines()[0] if text else ""
        return brief[:_MAX_WARNING_LENGTH]

    @classmethod
    def from_exception(
        cls,
        aspect: Aspect,
        error: object,
        table: object | None = None,
        prefix: str | None = None,
    ) -> "SnapshotWarning":
        """
        Build a warning from an exception (or message-like object).
        If prefix is not provided, a sensible default is chosen based on aspect.
        """
        base = prefix or cls._DEFAULT_PREFIX.get(aspect, "Failed to read metadata")
        brief = cls._format_exception_brief(error)
        message = f"{base}: {brief}" if brief else base
        return cls(table=table, aspect=aspect, message=message)

class CatalogStateReader(Protocol):
    def snapshot(self, request: SnapshotRequest) -> SnapshotResult: ...

