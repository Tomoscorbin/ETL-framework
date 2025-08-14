"""
Diagnostics primitives shared by the validator and rules.

- DiagnosticLevel: error/warning/info
- Diagnostic: a single validation finding
- ValidationReport: an immutable bag of diagnostics with a convenience .ok flag
"""

from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from typing import Tuple


class DiagnosticLevel(StrEnum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass(frozen=True)
class Diagnostic:
    """
    A single validation finding.

    table_key:
        Unescaped 'catalog.schema.table'. Use "" for global/no-table diagnostics.
    """
    table_key: str
    level: DiagnosticLevel
    code: str           # stable ID, e.g., "PK_COLUMNS_PRESENT"
    message: str        # one-line human-readable message
    hint: str = ""      # optional guidance


@dataclass(frozen=True)
class ValidationReport:
    """Immutable bag of diagnostics with a convenience 'ok' property."""
    diagnostics: Tuple[Diagnostic, ...]

    @property
    def ok(self) -> bool:
        return not any(d.level == DiagnosticLevel.ERROR for d in self.diagnostics)
