"""
Adapter: Column Comments Reader

Reads column comments for Delta tables from `information_schema.columns`,
one table at a time.

Flow
----
1) Inputs are FullyQualifiedTableName values (catalog.schema.table).
2) For each table:
   - Query `information_schema.columns` for that table.
   - Build a mapping of lowercased column name -> comment string ("" if missing).
3) On any failure, emit a SnapshotWarning with Aspect.COMMENTS and still
   produce an empty mapping for that table so the result is complete.

Notes:
-----
- Comments are returned separately from schema; the builder will merge them into
  ColumnState later (case-insensitive match by name).
- Keys are lowercased to avoid drift between catalog and StructField names.
"""
# TODO: optimise by reading from information schema once for all tables

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, NamedTuple

from pyspark.sql import SparkSession

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.state.adapters._sql_executor import select_column_comment_rows_for_table
from src.delta_engine.state.ports import Aspect, SnapshotWarning


class ColumnCommentsBatchReadResult(NamedTuple):
    """
    Aggregated result of reading column comments for a set of tables.

    Attributes:
    ----------
    comments_by_table :
        Mapping from FullyQualifiedTableName to {lowercased column -> comment string ("")} .
    warnings :
        Warnings raised while reading metadata (permissions, metastore issues, etc.).
    """

    comments_by_table: dict[FullyQualifiedTableName, dict[str, str]]
    warnings: list[SnapshotWarning]


class ColumnCommentsReader:
    """Read column comments from `information_schema.columns`, one table at a time."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialise the reader."""
        self.spark = spark

    def read_column_comments(
        self,
        full_table_names: tuple[FullyQualifiedTableName, ...],
    ) -> ColumnCommentsBatchReadResult:
        """
        Read column comments for each table in `full_table_names`.

        - On success: translate rows into a {lower_name -> comment} mapping ("" if None).
        - On failure: append a SnapshotWarning and default to {} for that table.
        """
        comments_by_table: dict[FullyQualifiedTableName, dict[str, str]] = {}
        warnings: list[SnapshotWarning] = []

        for full_table_name in full_table_names:
            try:
                rows = select_column_comment_rows_for_table(
                    self.spark,
                    catalog_name=full_table_name.catalog,
                    schema_name=full_table_name.schema,
                    table_name=full_table_name.table,
                )
                column_comments = build_column_comments_from_rows(rows)
                comments_by_table[full_table_name] = column_comments

            except Exception as error:
                warnings.append(
                    SnapshotWarning.from_exception(
                        aspect=Aspect.COMMENTS,
                        error=error,
                        full_table_name=full_table_name,
                        prefix="Failed to read column comments",
                    )
                )
                # Ensure every input has an entry
                comments_by_table.setdefault(full_table_name, {})

        return ColumnCommentsBatchReadResult(
            comments_by_table=comments_by_table,
            warnings=warnings,
        )


# ---------- helpers ----------


def build_column_comments_from_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    """
    Translate `information_schema.columns` rows into {lowercased column -> comment string}.

    Expected row fields:
      - column_name
      - comment (may be None)
    """
    out: dict[str, str] = {}
    for row in rows:
        raw_name = row.get("column_name")
        if raw_name is None:
            continue  # defensive
        lower_name = str(raw_name).lower()
        comment_value = row.get("comment")
        out[lower_name] = "" if comment_value is None else str(comment_value)
    return out
