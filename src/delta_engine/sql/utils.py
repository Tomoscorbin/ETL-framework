from __future__ import annotations

from collections.abc import Mapping

def escape_sql_literal(value: str) -> str:
    """
    Escape a Python string for use as a single-quoted SQL literal.
    Doubles single quotes per SQL rules. Empty/None → empty string.
    """
    return (value or "").replace("'", "''")

def format_tblproperties(props: Mapping[str, str]) -> str:
    """
    Format TBLPROPERTIES assignments: `'key' = 'value', 'k2' = 'v2'`.
    Keys/values are single-quoted SQL literals (NOT identifiers).
    Keys are sorted for deterministic output.
    """
    items = sorted(props.items(), key=lambda item: item[0])
    parts = [f"'{escape_sql_literal(k)}' = '{escape_sql_literal(v)}'" for k, v in items]
    return ", ".join(parts)
