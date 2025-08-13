"""
Adapter: Column Comments Reader

This module reads **column comments** for Delta tables from the catalog's
`information_schema.columns`, querying one table at a time.

High-level flow
---------------
1) Inputs are **FullyQualifiedTableName** values (catalog.schema.table).
2) For each table:
   - Query `information_schema.columns` for that specific table.
   - Build a mapping of **lowercased column name → comment string** ('' if missing).
3) On any failure for a table, emit a **SnapshotWarning** with `Aspect.COMMENTS`
   and still produce an **empty mapping** for that table so the output is complete.

Design notes
------------
- Comments are returned separately from schema; the builder will merge them into
  `ColumnState` later (e.g., match by name case-insensitively).
- Keys are **lowercased** to avoid case drift between Spark catalog and StructField names.
"""
# TODO: optimise by reading from information schema once for all tables

from __future__ import annotations

from typing import NamedTuple
from pyspark.sql import SparkSession

from src.delta_engine.state.ports import SnapshotWarning, Aspect
from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.state.adapters._sql_executor import select_column_comment_rows_for_table


class ColumnCommentsBatchReadResult(NamedTuple):
    """
    Aggregated result of reading column comments for a set of tables.

    Attributes
    ----------
    comments_by_table:
        Mapping from FullyQualifiedTableName to a dict of
        lowercased column name -> comment string (empty string if missing).
    warnings:
        Warnings raised while reading metadata (permissions, metastore issues, etc.).
    """
    comments_by_table: dict[FullyQualifiedTableName, dict[str, str]]
    warnings: list[SnapshotWarning]


class ColumnCommentsReader:
    """
    Read column comments from `information_schema.columns`, one table at a time.

    Inputs
    ------
    FullyQualifiedTableName values (catalog.schema.table).

    Outputs
    -------
    ColumnCommentsBatchReadResult with a complete map of inputs to
    {lowercased column name -> comment string}.
    """

    def __init__(self, spark: SparkSession) -> None:
        """Initialise the Reader."""
        self.spark = spark

    def read_column_comments(
        self,
        table_names: tuple[FullyQualifiedTableName, ...],
    ) -> ColumnCommentsBatchReadResult:
        """
        Read column comments for each table in `table_names`.

        For each FullyQualifiedTableName:
          - Query the catalog's information_schema.columns for that table.
          - Translate rows into a {lower_name -> comment} mapping ('' if None).
          - On query failure, emit a SnapshotWarning and default to {}.

        Parameters
        ----------
        table_names:
            A tuple of FullyQualifiedTableName values to inspect.

        Returns
        -------
        ColumnCommentsBatchReadResult
            {FQTN -> {lower_col -> comment}} and a list of warnings.
        """
        comments_by_table: dict[FullyQualifiedTableName, dict[str, str]] = {}
        warnings: list[SnapshotWarning] = []

        for name in table_names:
            try:
                rows = select_column_comment_rows_for_table(
                    self.spark,
                    name.catalog,
                    name.schema,
                    name.table,
                )
                comments = build_column_comments_from_rows(rows)
                comments_by_table[name] = comments

            except Exception as error:
                warning = SnapshotWarning.from_exception(
                        aspect=Aspect.COMMENTS,
                        error=error,
                        table=name,
                        prefix="Failed to read column comments",
                    )
                warnings.append(warning)

                # Still provide an entry so the result is complete
                comments_by_table.setdefault(name, {})

        return ColumnCommentsBatchReadResult(
            comments_by_table=comments_by_table,
            warnings=warnings,
        )


# ---------- helpers ----------

def build_column_comments_from_rows(rows) -> dict[str, str]:
    """
    Translate `information_schema.columns` rows into a mapping of
    lowercased column name -> comment string ('' if missing).

    Expected row fields:
    - column_name
    - comment  (may be None)
    """
    out: dict[str, str] = {}
    for row in rows:
        name_raw = row["column_name"]
        if name_raw is None:
            # Defensive: skip impossible rows
            continue
        name_lower = str(name_raw).lower()
        comment_value = row["comment"]
        out[name_lower] = "" if comment_value is None else str(comment_value)
    return out
