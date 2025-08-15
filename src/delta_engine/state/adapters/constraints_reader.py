"""
Adapter: Primary Key Reader

This module reads **primary key metadata** for Delta tables using the
catalog's `information_schema`, querying one table at a time.

High-level flow
---------------
1) Inputs are **FullyQualifiedTableName** values (catalog.schema.table).
2) For each table:
   - Run an information_schema query for that one table.
   - Collect primary key facts: constraint name and (ordinal_position, column_name) pairs.
   - If a constraint exists, order columns by `ordinal_position` and build a `PrimaryKeyState`.
   - If no constraint exists (zero rows or NULL name), record `None` for that table.
3) On any failure while reading a table's metadata, emit a **SnapshotWarning**
   with `Aspect.PRIMARY_KEY` and still produce `None` for that table to keep the
   result map complete.
4) Return a `PrimaryKeyBatchReadResult`:
   - `primary_key_by_table`: `{FullyQualifiedTableName -> PrimaryKeyState | None}`
   - `warnings`: a list of warnings encountered during the read
"""
# TODO: optimise by reading from information schema once for all tables

from __future__ import annotations

from typing import NamedTuple

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
    primary_key_by_table:
        A mapping from each requested FullyQualifiedTableName to either:
          - a PrimaryKeyState (constraint name + ordered columns), or
          - None, meaning “no primary key is defined” (or metadata not visible).
    warnings:
        A list of SnapshotWarning objects describing failures encountered
        while reading metadata. Absence of a PK does **not** produce a warning.
    """

    primary_key_by_table: dict[FullyQualifiedTableName, PrimaryKeyState | None]
    warnings: list[SnapshotWarning]


class ConstraintsReader:
    """
    Read primary key metadata from `information_schema`, one table at a time.

    Inputs
    ------
    FullyQualifiedTableName values (catalog.schema.table).

    Outputs
    -------
    PrimaryKeyBatchReadResult with a complete map of inputs to either a
    PrimaryKeyState (ordered by `ordinal_position`) or None, plus any warnings.

    Notes:
    -----
    - This reader does **not** determine whether the table exists. It treats
      “no rows” or a NULL constraint name as “no primary key.”
    - Failures to read metadata (e.g., permissions, metastore issues) are
      reported via SnapshotWarning with `Aspect.PRIMARY_KEY`.
    """

    def __init__(self, spark: SparkSession) -> None:
        """Initialise the Reader."""
        self.spark = spark

    def read_primary_keys(
        self,
        table_names: tuple[FullyQualifiedTableName, ...],
    ) -> PrimaryKeyBatchReadResult:
        """
        Read primary key metadata for each table in `table_names`.

        For each FullyQualifiedTableName:
          - Query the catalog's information_schema for that specific table.
          - Translate rows into a PrimaryKeyState (ordered columns) or None.
          - On query failure, emit a SnapshotWarning and default to None.

        Parameters
        ----------
        table_names:
            A tuple of FullyQualifiedTableName values to inspect.

        Returns:
        -------
        PrimaryKeyBatchReadResult
            {FQTN -> PrimaryKeyState | None} and a list of warnings.
        """
        primary_key_by_table: dict[FullyQualifiedTableName, PrimaryKeyState | None] = {}
        warnings: list[SnapshotWarning] = []

        for name in table_names:
            try:
                rows = select_primary_key_rows_for_table(
                    self.spark,
                    name.catalog,
                    name.schema,
                    name.table,
                )
                state = build_primary_key_state_from_rows(rows)
                primary_key_by_table[name] = state  # None means "no PK"

            except Exception as error:
                warning = SnapshotWarning.from_exception(
                    aspect=Aspect.PRIMARY_KEY,
                    error=error,
                    table=name,
                    prefix="Failed to read primary key",
                )
                warnings.append(warning)

                # On failure, still produce a value so callers always see a complete map
                primary_key_by_table.setdefault(name, None)

        return PrimaryKeyBatchReadResult(
            primary_key_by_table=primary_key_by_table,
            warnings=warnings,
        )


# ---------- helpers ----------


def build_primary_key_state_from_rows(rows) -> PrimaryKeyState | None:
    """
    Build a PrimaryKeyState from the rows returned for a single table.

    Behavior
    --------
    - If there are zero rows or the first row's `constraint_name` is NULL,
      return None (interpreted as "no primary key defined").
    - Otherwise, order column names by `ordinal_position` and return a
      PrimaryKeyState with the constraint name and ordered columns.

    Parameters
    ----------
    rows:
        Iterable of Row-like objects with fields:
        - constraint_name
        - column_name
        - ordinal_position

    Returns:
    -------
    PrimaryKeyState | None
        The constructed state if a constraint exists, otherwise None.
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

    ordered_columns = _ordered_column_names_by_position(entries)
    return PrimaryKeyState(name=str(constraint_name), columns=tuple(ordered_columns))


def _ordered_column_names_by_position(entries: list[tuple[int | None, str | None]]) -> list[str]:
    """
    Turn (ordinal_position, column_name) pairs into an ordered list of column names.

    Rules
    -----
    - Positions that are None are treated as 0 (defensive default).
    - Entries with a None `column_name` are ignored.
    - Output order is ascending by position for deterministic diffs.

    Parameters
    ----------
    entries:
        List of (ordinal_position, column_name) tuples.

    Returns:
    -------
    list[str]
        Column names ordered by `ordinal_position`.
    """
    normalized: list[tuple[int, str]] = []
    for position, name in entries:
        if name is None:
            continue
        normalized.append((0 if position is None else int(position), str(name)))

    normalized.sort(key=lambda pair: pair[0])

    ordered: list[str] = []
    for _, name in normalized:
        ordered.append(name)
    return ordered
