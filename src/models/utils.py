"""Utility functions used across models."""

from pyspark.sql import SparkSession


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


def check_primary_key_exists(
    spark: SparkSession,
    table_full_name: str,
    column_name: str,
) -> bool:
    """Check if a given column is defined as a primary key in a specified table."""
    catalog_name, schema_name, table_name = split_qualified_name(table_full_name)
    query = f"""
      SELECT kcu.column_name
      FROM {catalog_name}.information_schema.table_constraints tc
      JOIN {catalog_name}.information_schema.key_column_usage kcu
        ON  tc.constraint_catalog = kcu.constraint_catalog
        AND tc.constraint_schema  = kcu.constraint_schema
        AND tc.constraint_name    = kcu.constraint_name
      WHERE tc.constraint_type  = 'PRIMARY KEY'
        AND tc.table_schema     = '{schema_name}'
        AND tc.table_name       = '{table_name}'
        AND kcu.column_name     = '{column_name}'
    """
    return bool(spark.sql(query).limit(1).collect())
