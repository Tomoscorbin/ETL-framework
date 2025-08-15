"""Ports and value types for reading observed catalog state.

Defines:
- Aspects of state to read (schema, primary key, comments, properties)
- Snapshot request/response types
- Warning format with consistent, single-line messages
- Reader protocol for pluggable implementations
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar, Protocol

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.state.states import CatalogState

_MAX_WARNING_LENGTH = 300


class Aspect(StrEnum):
    SCHEMA = "SCHEMA"
    PRIMARY_KEY = "PRIMARY_KEY"
    COMMENTS = "COMMENTS"
    PROPERTIES = "PROPERTIES"


class SnapshotPolicy(StrEnum):
    PERMISSIVE = "permissive"
    STRICT = "strict"


@dataclass(frozen=True, slots=True)
class SnapshotRequest:
    """Describe what to snapshot from the catalog."""
    tables: tuple[FullyQualifiedTableName, ...]
    aspects: frozenset[Aspect]
    policy: SnapshotPolicy = SnapshotPolicy.PERMISSIVE


@dataclass(frozen=True, slots=True)
class SnapshotResult:
    """Point-in-time snapshot result plus any non-fatal warnings."""
    state: CatalogState
    warnings: tuple[SnapshotWarning, ...]


@dataclass(frozen=True, slots=True)
class SnapshotWarning:
    """
    A warning produced while snapshotting live catalog state.

    Attributes
    ----------
    full_table_name : FullyQualifiedTableName | None
        Which table the warning relates to (None if not table-specific).
    aspect : Aspect
        Which slice of metadata we were reading.
    message : str
        Short, single-line message (truncated to _MAX_WARNING_LENGTH).
    """

    full_table_name: FullyQualifiedTableName | None
    aspect: Aspect
    message: str

    # Defaults for each aspect (used when caller doesn't supply a custom prefix)
    _DEFAULT_PREFIX: ClassVar[dict[Aspect, str]] = {
        Aspect.SCHEMA: "Failed to read table schema",
        Aspect.PRIMARY_KEY: "Failed to read primary key metadata",
        Aspect.COMMENTS: "Failed to read comments",
        Aspect.PROPERTIES: "Failed to read table properties",
    }

    @classmethod
    def from_exception(
        cls,
        aspect: Aspect,
        error: object,
        full_table_name: FullyQualifiedTableName | None = None,
        prefix: str | None = None,
    ) -> "SnapshotWarning":
        """
        Build a warning from an exception (or message-like object).
        If `prefix` is not provided, a sensible default is chosen based on `aspect`.
        """
        base = prefix or cls._DEFAULT_PREFIX.get(aspect, "Failed to read metadata")
        brief = cls._format_exception_brief(error)
        message = f"{base}: {brief}" if brief else base
        return cls(full_table_name=full_table_name, aspect=aspect, message=message)
    
    @staticmethod
    def _format_exception_brief(error: object) -> str:
        """Return a trimmed, single-line summary for an exception or message-like object."""
        if isinstance(error, BaseException):
            name = type(error).__name__
            text = str(error).strip()
            first_line = text.splitlines()[0] if text else ""
            brief = f"{name}: {first_line}" if first_line else name
        else:
            text = str(error).strip()
            brief = text.splitlines()[0] if text else ""
        return brief[:_MAX_WARNING_LENGTH]


class CatalogStateReader(Protocol):
    """Port for implementations that can read catalog state."""
    def snapshot(self, request: SnapshotRequest) -> SnapshotResult: ...
    