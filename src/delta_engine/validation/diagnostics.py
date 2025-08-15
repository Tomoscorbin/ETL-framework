"""
Diagnostics primitives shared by the validator and rules.

- DiagnosticLevel: error/warning/info
- Diagnostic: a single validation finding
- ValidationReport: an immutable bag of diagnostics with a convenience .ok flag

Notes
-----
- `table_key` is the unescaped "catalog.schema.table". Use "" for global/no-table diagnostics.
- Prefer full words in codes (UPPER_SNAKE_CASE), e.g., "PRIMARY_KEY_COLUMNS_PRESENT".
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TypeAlias


# Explicit alias for readability across the codebase.
TableKey: TypeAlias = str


class DiagnosticLevel(StrEnum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass(frozen=True, slots=True)
class Diagnostic:
    """
    A single validation finding.

    table_key:
        Unescaped 'catalog.schema.table'. Use "" for global/no-table diagnostics.
    code:
        Stable identifier in UPPER_SNAKE_CASE with full words (no abbreviations),
        e.g., "PRIMARY_KEY_COLUMNS_PRESENT".
    message:
        One-line human-readable message.
    hint:
        Optional guidance; empty string means "no hint".
    """
    table_key: TableKey
    level: DiagnosticLevel
    code: str
    message: str
    hint: str = ""


@dataclass(frozen=True, slots=True)
class ValidationReport:
    """Immutable bag of diagnostics with a convenience 'ok' property."""

    diagnostics: tuple[Diagnostic, ...]

    @property
    def ok(self) -> bool:
        return not any(d.level == DiagnosticLevel.ERROR for d in self.diagnostics)
