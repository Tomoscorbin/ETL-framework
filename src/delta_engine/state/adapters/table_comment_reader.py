"""
Adapter: Table Comment Reader

This module reads the **table-level comment** for Delta tables using the
catalog's `information_schema.tables` view, querying one table at a time.

High-level flow
---------------
1) Inputs are **FullyQualifiedTableName** values (catalog.schema.table).
2) For each table:
   - Call a dedicated executor that builds and runs a information_schema query.
   - Extract the table-level `comment` (or return an empty string if absent).
3) On any failure for a table:
   - Emit a **SnapshotWarning** with `Aspect.COMMENTS` (table-level).
   - Still produce an entry (empty string) so the result map is complete.

Design notes
------------
- Column comments are handled by a separate reader (ColumnCommentsReader).
- This reader only returns the table-level comment as a string.
"""
# TODO: optimise by reading from information schema once for all tables

from __future__ import annotations

from typing import NamedTuple

from pyspark.sql import SparkSession

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.state.adapters._sql_executor import select_table_comment_rows_for_table
from src.delta_engine.state.ports import Aspect, SnapshotWarning


class TableCommentBatchReadResult(NamedTuple):
    """
    Aggregated result of reading table-level comments for a set of tables.

    Attributes:
    ----------
    comment_by_table:
        Mapping from FullyQualifiedTableName to the table comment (empty string if missing).
    warnings:
        Warnings raised while reading metadata (permissions, metastore issues, etc.).
    """

    comment_by_table: dict[FullyQualifiedTableName, str]
    warnings: list[SnapshotWarning]


class TableCommentReader:
    """
    Read table-level comments from `information_schema.tables`, one table at a time.

    Inputs
    ------
    FullyQualifiedTableName values (catalog.schema.table).

    Outputs
    -------
    TableCommentBatchReadResult with a complete map of inputs to table comments.
    """

    def __init__(self, spark: SparkSession) -> None:
        """Initialise the Reader."""
        self.spark = spark

    def read_table_comments(
        self,
        table_names: tuple[FullyQualifiedTableName, ...],
    ) -> TableCommentBatchReadResult:
        """
        Read the table-level comment for each table in `table_names`.

        Behavior
        --------
        - On success: extract `comment` from the information_schema row; default to "" if NULL.
        - On failure: append a SnapshotWarning and default to "" for that table.

        Returns:
        -------
        TableCommentBatchReadResult
            {FQTN -> comment string}, plus warnings.
        """
        comment_by_table: dict[FullyQualifiedTableName, str] = {}
        warnings: list[SnapshotWarning] = []

        for name in table_names:
            try:
                rows = select_table_comment_rows_for_table(
                    self.spark,
                    catalog=name.catalog,
                    schema=name.schema,
                    table=name.table,
                )
                comment_by_table[name] = build_table_comment_from_rows(rows)

            except Exception as error:
                warning = SnapshotWarning.from_exception(
                    aspect=Aspect.COMMENTS,
                    error=error,
                    table=name,
                    prefix="Failed to read table comment",
                )
                warnings.append(warning)

                # On failure, still produce a value so callers always see a complete map
                comment_by_table.setdefault(name, "")

        return TableCommentBatchReadResult(
            comment_by_table=comment_by_table,
            warnings=warnings,
        )


# ---------- helpers ----------


def build_table_comment_from_rows(rows) -> str:
    """
    Translate `information_schema.tables` rows into a single table comment string.

    Expected row shape: one row at most for a specific table, with a `comment` field.
    - No rows   -> "" (no comment / table not visible).
    - NULL      -> "" (comment not set).
    - Otherwise -> the string value.
    """
    if not rows:
        return ""
    value = rows[0]["comment"]
    return "" if value is None else str(value)
