"""Utility functions used across models."""


def to_pascal_case(s: str) -> str:
    """Convert a snake_case string to PascalCase."""
    return "".join(word.capitalize() for word in s.split("_"))


def split_qualified_name(qualified_name: str) -> tuple[str, str, str]:
    """
    Split a 3-part identifier like "catalog.schema.table" into its components.
    Raises ValueError if it doesn't have exactly three parts.
    """
    parts = qualified_name.split(".", 2)
    if len(parts) != 3:
        raise ValueError(f"Expected 3-part name, got {len(parts)}: {qualified_name!r}")
    catalog, schema, table = parts
    return catalog, schema, table
