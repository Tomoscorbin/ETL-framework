"""
Adapter: Table Comment Reader

Reads the table-level comment for Delta tables using
`information_schema.tables`, one table at a time.

Flow
----
1) Inputs are FullyQualifiedTableName values (catalog.schema.table).
2) For each table:
   - Run the information_schema query via an executor.
   - Extract the table-level `comment` (empty string if absent/NULL).
3) On failure:
   - Emit a SnapshotWarning with Aspect.COMMENTS.
   - Still produce an empty-string entry so the result map is complete.

Notes:
-----
- Column comments are handled by a separate reader (ColumnCommentsReader).
- This reader only returns the table-level comment as a string.
"""
# TODO: optimise by reading from information schema once for all tables

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, NamedTuple

from pyspark.sql import SparkSession

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.state.adapters._sql_executor import select_table_comment_rows_for_table
from src.delta_engine.state.ports import Aspect, SnapshotWarning


class TableCommentBatchReadResult(NamedTuple):
    """
    Aggregated result of reading table-level comments for a set of tables.

    Attributes:
    ----------
    comment_by_table :
        Mapping from FullyQualifiedTableName to the table comment ("" if missing).
    warnings :
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
        """Initialise the reader."""
        self.spark = spark

    def read_table_comments(
        self,
        full_table_names: tuple[FullyQualifiedTableName, ...],
    ) -> TableCommentBatchReadResult:
        """
        Read the table-level comment for each table in `full_table_names`.

        - On success: extract `comment` from the information_schema row; default to "" if NULL.
        - On failure: append a SnapshotWarning and default to "" for that table.
        """
        comment_by_table: dict[FullyQualifiedTableName, str] = {}
        warnings: list[SnapshotWarning] = []

        for full_table_name in full_table_names:
            try:
                rows = select_table_comment_rows_for_table(
                    self.spark,
                    catalog_name=full_table_name.catalog,
                    schema_name=full_table_name.schema,
                    table_name=full_table_name.table,
                )
                comment_by_table[full_table_name] = build_table_comment_from_rows(rows)
            except Exception as error:
                warnings.append(
                    SnapshotWarning.from_exception(
                        aspect=Aspect.COMMENTS,
                        error=error,
                        full_table_name=full_table_name,
                        prefix="Failed to read table comment",
                    )
                )
                # Ensure every input has an entry
                comment_by_table.setdefault(full_table_name, "")

        return TableCommentBatchReadResult(
            comment_by_table=comment_by_table,
            warnings=warnings,
        )


# ---------- helpers ----------


def build_table_comment_from_rows(rows: Sequence[Mapping[str, Any]]) -> str:
    """
    Translate `information_schema.tables` rows into a single table comment string.

    Expected row shape: at most one row for the table, with a `comment` field.
    - No rows   -> "" (no comment / table not visible).
    - NULL      -> "" (comment not set).
    - Otherwise -> the string value of `comment`.
    """
    if not rows:
        return ""
    value = rows[0]["comment"]
    return "" if value is None else str(value)
