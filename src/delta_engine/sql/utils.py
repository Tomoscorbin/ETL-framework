"""SQL string helpers."""

from __future__ import annotations

from collections.abc import Mapping

def escape_sql_literal(value: str) -> str:
    """
    Escape a Python string for use as a single-quoted SQL literal.
    Doubles single quotes per SQL rules. Empty/None → empty string.
    """
    return (value or "").replace("'", "''")
