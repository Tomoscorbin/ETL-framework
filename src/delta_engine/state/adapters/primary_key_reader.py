"""
Adapter: Primary Key Reader

Reads primary key metadata for Delta tables using `information_schema`, one table at a time.

Flow
----
1) Inputs are FullyQualifiedTableName values (catalog.schema.table).
2) For each table:
   - Query the catalog's information_schema for that specific table.
   - Collect (constraint_name, ordinal_position, column_name) rows.
   - If a constraint exists, order columns by `ordinal_position` and build a PrimaryKeyState.
   - If no constraint exists (zero rows or NULL name), record None.
3) On any failure for a table:
   - Emit a SnapshotWarning with Aspect.PRIMARY_KEY.
   - Still produce None so the result map is complete.
"""
# TODO: optimise by reading from information schema once for all tables

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, NamedTuple

from pyspark.sql import SparkSession

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.state.adapters._sql_executor import select_primary_key_rows_for_table
from src.delta_engine.state.ports import Aspect, SnapshotWarning
from src.delta_engine.state.states import PrimaryKeyState


class PrimaryKeyBatchReadResult(NamedTuple):
    """
    Aggregated result of reading primary keys for a set of tables.

    Attributes:
    ----------
    primary_key_by_table :
        {FullyQualifiedTableName -> PrimaryKeyState | None}
    warnings :
        Non-fatal warnings encountered while reading. Absence of a PK is not a warning.
    """

    primary_key_by_table: dict[FullyQualifiedTableName, PrimaryKeyState | None]
    warnings: list[SnapshotWarning]


class PrimaryKeyReader:
    """
    Read primary key metadata from `information_schema`, one table at a time.
    """

    def __init__(self, spark: SparkSession) -> None:
        """Initialise the reader."""
        self.spark = spark

    def read_primary_keys(
        self,
        full_table_names: tuple[FullyQualifiedTableName, ...],
    ) -> PrimaryKeyBatchReadResult:
        """
        Read primary key metadata for each table in `full_table_names`.

        - For each table: run the information_schema query and translate rows
          into a PrimaryKeyState (ordered columns) or None.
        - On query failure: emit a SnapshotWarning and default to None.
        """
        primary_key_by_table: dict[FullyQualifiedTableName, PrimaryKeyState | None] = {}
        warnings: list[SnapshotWarning] = []

        for full_table_name in full_table_names:
            try:
                rows = select_primary_key_rows_for_table(
                    self.spark,
                    catalog_name=full_table_name.catalog,
                    schema_name=full_table_name.schema,
                    table_name=full_table_name.table,
                )
                state = build_primary_key_state_from_rows(rows)
                primary_key_by_table[full_table_name] = state  # None means "no PK"

            except Exception as error:
                warnings.append(
                    SnapshotWarning.from_exception(
                        aspect=Aspect.PRIMARY_KEY,
                        error=error,
                        full_table_name=full_table_name,
                        prefix="Failed to read primary key",
                    )
                )
                # Ensure every input has an entry
                primary_key_by_table.setdefault(full_table_name, None)

        return PrimaryKeyBatchReadResult(
            primary_key_by_table=primary_key_by_table,
            warnings=warnings,
        )


# ---------- helpers ----------


def build_primary_key_state_from_rows(rows: Iterable[Any]) -> PrimaryKeyState | None:
    """
    Build a PrimaryKeyState from rows for a single table.

    Rules
    -----
    - If there are zero rows or the first row's `constraint_name` is NULL/empty,
      return None (interpreted as "no primary key defined").
    - Otherwise, order column names by `ordinal_position` and return a PrimaryKeyState.

    Expected row fields:
      - constraint_name
      - column_name
      - ordinal_position
    """
    constraint_name: str | None = None
    entries: list[tuple[int | None, str | None]] = []

    for row in rows:
        if constraint_name is None:
            # The constraint name should be the same across rows; take the first.
            constraint_name = row["constraint_name"]
        entries.append((row["ordinal_position"], row["column_name"]))

    if not constraint_name:
        return None

    ordered_columns = _order_column_names_by_position(entries)
    return PrimaryKeyState(name=str(constraint_name), columns=tuple(ordered_columns))


def _order_column_names_by_position(entries: list[tuple[int | None, str | None]]) -> list[str]:
    """
    Turn (ordinal_position, column_name) pairs into an ordered list of column names.

    - Positions that are None are treated as 0 (defensive default).
    - Entries with a None column_name are ignored.
    - Output order is ascending by position for deterministic diffs.
    """
    normalized = [
        (0 if position is None else int(position), str(name))
        for position, name in entries
        if name is not None
    ]
    normalized.sort(key=lambda pair: pair[0])
    return [name for _, name in normalized]
