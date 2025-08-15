"""
Adapter: Table Properties Reader

This module reads **Delta table properties** (the `configuration` map) by calling
Delta Lake metadata for one table at a time.

High-level flow
---------------
1) Inputs are **FullyQualifiedTableName** values (catalog.schema.table).
2) For each table:
   - Invoke an executor that uses DeltaTable.detail() to fetch the
     `configuration` map (table properties).
   - Coerce keys and values to strings and return a plain dict.
3) On any failure (e.g., permissions, table missing, non-Delta), emit a
   **SnapshotWarning** with `Aspect.PROPERTIES` and return an empty dict for that table.

Design notes
------------
- We use the Delta API (DeltaTable.detail()) instead of SQL because the
  configuration map is surfaced directly and reliably there.
- The executor does not catch exceptions; the reader owns warning creation so
  messaging is consistent across adapters.
"""

from __future__ import annotations

from typing import NamedTuple

from pyspark.sql import SparkSession

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.models import TableProperty
from src.delta_engine.state.adapters._delta_executor import load_table_properties_map
from src.delta_engine.state.ports import Aspect, SnapshotWarning

ALLOWED_TABLE_PROPERTY_KEYS: frozenset[str] = frozenset(k.value for k in TableProperty)


class TablePropertiesBatchReadResult(NamedTuple):
    """
    Aggregated result of reading table properties for a set of tables.

    Attributes:
    ----------
    properties_by_table:
        Mapping from FullyQualifiedTableName to a dict of {property_name -> value}.
        (Empty dict if no properties or on failure.)
    warnings:
        Warnings raised while reading metadata (permissions, table missing, etc.).
    """

    properties_by_table: dict[FullyQualifiedTableName, dict[str, str]]
    warnings: list[SnapshotWarning]


class TablePropertiesReader:
    """
    Read Delta table properties via DeltaTable.detail(), one table at a time.

    Inputs
    ------
    FullyQualifiedTableName values (catalog.schema.table).

    Outputs
    -------
    TablePropertiesBatchReadResult with a complete map of inputs to property dicts.
    """

    def __init__(self, spark: SparkSession) -> None:
        """Store the Spark session used to access Delta metadata."""
        self.spark = spark

    def read_table_properties(
        self,
        table_names: tuple[FullyQualifiedTableName, ...],
    ) -> TablePropertiesBatchReadResult:
        """
        Read table properties for each table in `table_names`.

        Behavior
        --------
        - On success: return a dict of stringified key/value pairs.
        - On failure: append a SnapshotWarning and default to {} for that table.
        """
        properties_by_table: dict[FullyQualifiedTableName, dict[str, str]] = {}
        warnings: list[SnapshotWarning] = []

        for name in table_names:
            try:
                properties = load_table_properties_map(
                    self.spark,
                    catalog=name.catalog,
                    schema=name.schema,
                    table=name.table,
                )
                allowed_properties = _filter_allowed_properties(properties)
                properties_by_table[name] = allowed_properties
            except Exception as error:
                warning = SnapshotWarning.from_exception(
                    aspect=Aspect.PROPERTIES,
                    error=error,
                    table=name,
                    prefix="Failed to read table properties",
                )
                warnings.append(warning)

                # On failure, still produce a value so callers always see a complete map
                properties_by_table.setdefault(name, {})

        return TablePropertiesBatchReadResult(
            properties_by_table=properties_by_table,
            warnings=warnings,
        )


def _filter_allowed_properties(properties: dict[str, str]) -> dict[str, str]:
    return {key: value for key, value in properties.items() if key in ALLOWED_TABLE_PROPERTY_KEYS}
