"""
Adapter: Catalog Reader

This orchestrates a catalog snapshot by delegating to focused readers:

- SchemaReader              → existence + physical columns (no comments)
- ColumnCommentsReader      → per-column comments (lowercased keys)
- TableCommentReader        → table-level comment (string)
- TablePropertiesReader     → table properties (Delta configuration map)
- ConstraintsReader         → primary key (name + ordered columns)

Flow
----
1) Always read existence (SchemaReader). Optionally read physical schema.
2) If COMMENTS is requested:
   - read column comments (only useful if schema is requested, so we can merge),
   - read table comment.
3) If PROPERTIES is requested: read properties.
4) If PRIMARY_KEY is requested: read primary keys.
5) Hand all slices to TableStateBuilder, which:
   - merges column comments into ColumnState (case-insensitive),
   - includes table comment, properties, primary key,
   - keys CatalogState by unescaped 'catalog.schema.table' using identifiers helpers.
6) Return CatalogState + warnings. Under STRICT policy, raise if any warnings.

Design rules
------------
- No ad-hoc name assembly here. All un/quoted name rendering is centralised in `identifiers`.
- Readers NEVER merge; `_builder` is the single place that combines slices.
- Absences (e.g., no PK) are represented as None/empty values, not warnings.
"""

from __future__ import annotations

from typing import List
from pyspark.sql import SparkSession

from ..ports import (
    SnapshotRequest,
    SnapshotResult,
    SnapshotWarning,
    SnapshotPolicy,
    Aspect,
)
from src.delta_engine.state.adapters.schema_reader import SchemaReader
from src.delta_engine.state.adapters.constraints_reader import ConstraintsReader
from src.delta_engine.state.adapters.column_comments_reader import ColumnCommentsReader
from src.delta_engine.state.adapters.table_comment_reader import TableCommentReader
from src.delta_engine.state.adapters.table_properties_reader import TablePropertiesReader
from src.delta_engine.state.adapters._builder import TableStateBuilder


class CatalogReader:
    """
    Public orchestrator that produces a CatalogState from requested aspects.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.schema_reader = SchemaReader(spark)
        self.constraints_reader = ConstraintsReader(spark)
        self.column_comments_reader = ColumnCommentsReader(spark)
        self.table_comment_reader = TableCommentReader(spark)
        self.table_properties_reader = TablePropertiesReader(spark)
        self.builder = TableStateBuilder()

    # ---------- public API ----------

    def snapshot(self, request: SnapshotRequest) -> SnapshotResult:
        """
        Build a CatalogState for the requested tables and aspects.

        - Always reads existence.
        - Reads physical schema only when Aspect.SCHEMA is requested.
        - Reads column comments only when both Aspect.COMMENTS and Aspect.SCHEMA are requested.
        - Reads table comment when Aspect.COMMENTS is requested.
        - Reads properties when Aspect.PROPERTIES is requested.
        - Reads primary keys when Aspect.PRIMARY_KEY is requested.
        - On STRICT policy, raises if any warnings were produced.
        """
        tables = request.tables
        aspects = request.aspects
        warnings: List[SnapshotWarning] = []

        # 1) existence (+ optional physical schema)
        existence_by_table, columns_by_table, w1 = self._step_schema(
            tables=tables,
            include_schema=(Aspect.SCHEMA in aspects),
        )
        warnings.extend(w1)

        # 2) per-column comments (only if schema present to merge)
        column_comments_by_table, w2 = self._step_column_comments(
            tables=tables,
            enabled=(Aspect.COMMENTS in aspects and Aspect.SCHEMA in aspects),
        )
        warnings.extend(w2)

        # 3) table-level comment
        table_comment_by_table, w3 = self._step_table_comment(
            tables=tables,
            enabled=(Aspect.COMMENTS in aspects),
        )
        warnings.extend(w3)

        # 4) table properties
        properties_by_table, w4 = self._step_properties(
            tables=tables,
            enabled=(Aspect.PROPERTIES in aspects),
        )
        warnings.extend(w4)

        # 5) primary key
        primary_keys_by_table, w5 = self._step_primary_keys(
            tables=tables,
            enabled=(Aspect.PRIMARY_KEY in aspects),
        )
        warnings.extend(w5)

        # 6) assemble final state
        catalog_state: CatalogState = self.builder.assemble(
            tables=tables,
            exists=existence_by_table,
            schema=columns_by_table,
            column_comments=column_comments_by_table,
            table_comments=table_comment_by_table,
            properties=properties_by_table,
            primary_keys=primary_keys_by_table,
        )

        # 7) STRICT policy handling
        if request.policy is SnapshotPolicy.STRICT and warnings:
            raise RuntimeError(f"Snapshot produced {len(warnings)} warning(s)")

        return SnapshotResult(state=catalog_state, warnings=tuple(warnings))

    # ---------- slice methods (single-purpose, easy to test) ----------

    def _step_schema(
        self,
        *,
        tables: tuple[FullyQualifiedTableName, ...],
        include_schema: bool,
    ) -> tuple[
        Dict[FullyQualifiedTableName, bool],
        Dict[FullyQualifiedTableName, tuple[ColumnState, ...]],
        List[SnapshotWarning],
    ]:
        """
        Read existence for all tables, and optionally their physical schema.
        Returns (existence_by_table, columns_by_table, warnings).
        """
        result = self.schema_reader.read_schema(tables, include_schema)
        return (
            result.existence_by_table,
            result.columns_by_table,
            list(result.warnings),
        )

    def _step_column_comments(
        self,
        *,
        tables: tuple[FullyQualifiedTableName, ...],
        enabled: bool,
    ) -> tuple[Dict[FullyQualifiedTableName, Dict[str, str]], List[SnapshotWarning]]:
        """
        Read per-column comments as {lower_name -> comment}. If disabled, return empty dicts.
        """
        if not enabled:
            return ({t: {} for t in tables}, [])
        result = self.column_comments_reader.read_column_comments(tables)
        return (result.comments_by_table, list(result.warnings))

    def _step_table_comment(
        self,
        *,
        tables: tuple[FullyQualifiedTableName, ...],
        enabled: bool,
    ) -> tuple[Dict[FullyQualifiedTableName, str], List[SnapshotWarning]]:
        """
        Read table-level comments as strings. If disabled, return empty strings.
        """
        if not enabled:
            return ({t: "" for t in tables}, [])
        result = self.table_comment_reader.read_table_comments(tables)
        return (result.comment_by_table, list(result.warnings))

    def _step_properties(
        self,
        *,
        tables: tuple[FullyQualifiedTableName, ...],
        enabled: bool,
    ) -> tuple[Dict[FullyQualifiedTableName, Dict[str, str]], List[SnapshotWarning]]:
        """
        Read table properties (Delta configuration) as {key -> value}. If disabled, return empty dicts.
        """
        if not enabled:
            return ({t: {} for t in tables}, [])
        result = self.table_properties_reader.read_table_properties(tables)
        return (result.properties_by_table, list(result.warnings))

    def _step_primary_keys(
        self,
        *,
        tables: tuple[FullyQualifiedTableName, ...],
        enabled: bool,
    ) -> tuple[Dict[FullyQualifiedTableName, PrimaryKeyState | None], List[SnapshotWarning]]:
        """
        Read primary keys (name + ordered columns). If disabled, return None for all.
        """
        if not enabled:
            return ({t: None for t in tables}, [])
        result = self.constraints_reader.read_primary_keys(tables)
        return (result.primary_key_by_table, list(result.warnings))
