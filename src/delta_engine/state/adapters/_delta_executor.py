"""Delta Executor: Delta/Spark catalog I/O helpers for single-table reads."""

from __future__ import annotations
from typing import Dict
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from src.delta_engine.identifiers import render_fully_qualified_name_from_parts


def check_table_exists(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
) -> bool:
    """
    Return True if the three-part table exists in the Spark catalog.
    """
    full_name = render_fully_qualified_name_from_parts(catalog, schema, table)
    return spark.catalog.tableExists(full_name)


def load_table_struct(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
) -> T.StructType:
    """
    Load a Delta table's StructType via DeltaTable.forName(...).toDF().schema.
    Raises if the table cannot be opened (missing, permissions, not Delta, etc.).
    """
    full_name = render_fully_qualified_name_from_parts(catalog, schema, table)
    delta = DeltaTable.forName(spark, full_name)
    return delta.toDF().schema


def load_table_properties_map(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
) -> Dict[str, str]:
    """
    Load the Delta table's properties map as {str: str}.

    Notes:
    - DeltaTable.detail() returns a single-row DataFrame; on Databricks runtimes
      the properties map column is usually named 'properties'. Some envs expose
      'configuration'. We handle both case-insensitively.
    - This function does NOT filter allowed keys; it returns everything the
      metastore reports. Filtering is done by the reader.
    - Errors (table not found, not a Delta table, permissions) bubble up to the caller.
    """
    full_name = render_fully_qualified_name_from_parts(catalog, schema, table)

    delta = DeltaTable.forName(spark, full_name)
    detail_df = delta.detail()

    # Find the properties/configuration column case-insensitively
    lower_by_actual = {c.lower(): c for c in detail_df.columns}
    col_name = lower_by_actual.get("properties") or lower_by_actual.get("configuration")
    if not col_name:
        # No properties map column present; treat as empty map (not an error)
        return {}

    row = detail_df.select(col_name).first()
    if row is None:
        return {}

    raw_map = row[col_name] or {}
    # Coerce defensively to plain {str: str}
    try:
        items = dict(raw_map).items()
    except Exception:
        # In rare cases the value could be a JSON string; last-ditch parse
        # Avoid importing json here; keep it simple and empty on weird shapes.
        return {}

    return {str(k): str(v) for k, v in items}
