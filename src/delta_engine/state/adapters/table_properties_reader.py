"""
Adapter: Table Properties Reader

Reads Delta table properties (the `configuration` map) via DeltaTable.detail().

Flow
----
1) Inputs are FullyQualifiedTableName values (catalog.schema.table).
2) For each table:
   - Use an executor to fetch the configuration map.
   - Normalize keys/values to strings, then filter to allowed property keys.
3) On any failure, emit a SnapshotWarning with Aspect.PROPERTIES and return {} for that table.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, NamedTuple

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
    properties_by_table : dict[FullyQualifiedTableName, dict[str, str]]
        Mapping from table to {property_name -> value}. Empty dict on failure.
    warnings : list[SnapshotWarning]
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
    TablePropertiesBatchReadResult mapping each input to a normalized property dict.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def read_table_properties(
        self,
        full_table_names: tuple[FullyQualifiedTableName, ...],
    ) -> TablePropertiesBatchReadResult:
        """
        Read table properties for each table in `full_table_names`.

        - On success: return a dict of normalized (string) key/value pairs filtered to allowed keys.
        - On failure: append a SnapshotWarning and default to {} for that table.
        """
        properties_by_table: dict[FullyQualifiedTableName, dict[str, str]] = {}
        warnings: list[SnapshotWarning] = []

        for full_table_name in full_table_names:
            try:
                raw_properties = load_table_properties_map(
                    self.spark,
                    catalog=full_table_name.catalog,
                    schema=full_table_name.schema,
                    table=full_table_name.table,
                )
                normalized = _normalize_properties(raw_properties)
                allowed = _filter_allowed_properties(normalized)
                properties_by_table[full_table_name] = allowed
            except Exception as error:
                warnings.append(
                    SnapshotWarning.from_exception(
                        aspect=Aspect.PROPERTIES,
                        error=error,
                        full_table_name=full_table_name,
                        prefix="Failed to read table properties",
                    )
                )
                # ensure every input has an entry
                properties_by_table.setdefault(full_table_name, {})

        return TablePropertiesBatchReadResult(
            properties_by_table=properties_by_table,
            warnings=warnings,
        )


# -----------------
# Helpers
# -----------------


def _normalize_properties(props: Mapping[Any, Any]) -> dict[str, str]:
    """
    Coerce mapping keys/values to strings; supports TableProperty keys.
    """
    normalized: dict[str, str] = {}
    for k, v in dict(props).items():
        key = k.value if isinstance(k, TableProperty) else str(k)
        normalized[key] = str(v)
    return normalized


def _filter_allowed_properties(properties: Mapping[str, str]) -> dict[str, str]:
    """Keep only properties explicitly allowed by TableProperty."""
    return {key: value for key, value in properties.items() if key in ALLOWED_TABLE_PROPERTY_KEYS}
