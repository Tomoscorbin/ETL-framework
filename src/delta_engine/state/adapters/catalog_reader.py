"""
Adapter: Catalog Reader

Orchestrates a catalog snapshot by delegating to focused readers:

- SchemaReader           → existence + physical columns (no comments)
- ColumnCommentsReader   → per-column comments (lowercased keys)
- TableCommentReader     → table-level comment (string)
- TablePropertiesReader  → table properties (Delta configuration map)
- PrimaryKeyReader       → primary key (name + ordered columns)

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
   - keys CatalogState by FullyQualifiedTableName.
6) Return CatalogState + warnings. Under STRICT policy, raise if any warnings.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from src.delta_engine.identifiers import FullyQualifiedTableName
from src.delta_engine.state.adapters._builder import TableStateBuilder
from src.delta_engine.state.adapters.column_comments_reader import ColumnCommentsReader
from src.delta_engine.state.adapters.primary_key_reader import PrimaryKeyReader
from src.delta_engine.state.adapters.schema_reader import SchemaReader
from src.delta_engine.state.adapters.table_comment_reader import TableCommentReader
from src.delta_engine.state.adapters.table_properties_reader import TablePropertiesReader
from src.delta_engine.state.ports import (
    Aspect,
    SnapshotPolicy,
    SnapshotRequest,
    SnapshotResult,
    SnapshotWarning,
)
from src.delta_engine.state.states import CatalogState, ColumnState, PrimaryKeyState


class CatalogReader:
    """Public orchestrator that produces a CatalogState from requested aspects."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.schema_reader = SchemaReader(spark)
        self.primary_key_reader = PrimaryKeyReader(spark)
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
        full_table_names = request.tables
        aspects = request.aspects
        warnings: list[SnapshotWarning] = []

        # 1) existence (+ optional physical schema)
        existence_by_table, columns_by_table, warnings_schema = self._step_schema(
            tables=full_table_names,
            include_schema=(Aspect.SCHEMA in aspects),
        )
        warnings.extend(warnings_schema)

        # 2) per-column comments (only if schema present to merge)
        column_comments_by_table, warnings_column_comments = self._step_column_comments(
            tables=full_table_names,
            enabled=(Aspect.COMMENTS in aspects and Aspect.SCHEMA in aspects),
        )
        warnings.extend(warnings_column_comments)

        # 3) table-level comment
        table_comment_by_table, warnings_table_comment = self._step_table_comment(
            tables=full_table_names,
            enabled=(Aspect.COMMENTS in aspects),
        )
        warnings.extend(warnings_table_comment)

        # 4) table properties
        properties_by_table, warnings_properties = self._step_properties(
            tables=full_table_names,
            enabled=(Aspect.PROPERTIES in aspects),
        )
        warnings.extend(warnings_properties)

        # 5) primary key
        primary_keys_by_table, warnings_primary_keys = self._step_primary_keys(
            tables=full_table_names,
            enabled=(Aspect.PRIMARY_KEY in aspects),
        )
        warnings.extend(warnings_primary_keys)

        # 6) assemble final state
        catalog_state: CatalogState = self.builder.assemble(
            tables=full_table_names,
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

    # ---------- slice methods ----------

    def _step_schema(
        self,
        *,
        tables: tuple[FullyQualifiedTableName, ...],
        include_schema: bool,
    ) -> tuple[
        dict[FullyQualifiedTableName, bool],
        dict[FullyQualifiedTableName, tuple[ColumnState, ...]],
        list[SnapshotWarning],
    ]:
        """Read existence for all tables, and optionally their physical schema."""
        result = self.schema_reader.read_schema(tables, include_schema)
        existence_by_table = result.existence_by_table
        columns_by_table = result.columns_by_table
        warnings = list(result.warnings)
        return existence_by_table, columns_by_table, warnings

    def _step_column_comments(
        self,
        *,
        tables: tuple[FullyQualifiedTableName, ...],
        enabled: bool,
    ) -> tuple[dict[FullyQualifiedTableName, dict[str, str]], list[SnapshotWarning]]:
        """Read per-column comments as {lower_name -> comment}. If disabled, return empty dicts."""
        if not enabled:
            empty_map = {t: {} for t in tables}
            return empty_map, []
        result = self.column_comments_reader.read_column_comments(tables)
        comments_by_table = result.comments_by_table
        warnings = list(result.warnings)
        return comments_by_table, warnings

    def _step_table_comment(
        self,
        *,
        tables: tuple[FullyQualifiedTableName, ...],
        enabled: bool,
    ) -> tuple[dict[FullyQualifiedTableName, str], list[SnapshotWarning]]:
        """Read table-level comments as strings. If disabled, return empty strings."""
        if not enabled:
            empty_comments = dict.fromkeys(tables, "")
            return empty_comments, []
        result = self.table_comment_reader.read_table_comments(tables)
        comment_by_table = result.comment_by_table
        warnings = list(result.warnings)
        return comment_by_table, warnings

    def _step_properties(
        self,
        *,
        tables: tuple[FullyQualifiedTableName, ...],
        enabled: bool,
    ) -> tuple[dict[FullyQualifiedTableName, dict[str, str]], list[SnapshotWarning]]:
        """Read table properties (Delta configuration) as {key -> value}. If disabled, return empty dicts."""
        if not enabled:
            empty_map = {t: {} for t in tables}
            return empty_map, []
        result = self.table_properties_reader.read_table_properties(tables)
        properties_by_table = result.properties_by_table
        warnings = list(result.warnings)
        return properties_by_table, warnings

    def _step_primary_keys(
        self,
        *,
        tables: tuple[FullyQualifiedTableName, ...],
        enabled: bool,
    ) -> tuple[dict[FullyQualifiedTableName, PrimaryKeyState | None], list[SnapshotWarning]]:
        """Read primary keys (name + ordered columns). If disabled, return None for all."""
        if not enabled:
            none_map = dict.fromkeys(tables)
            return none_map, []
        result = self.primary_key_reader.read_primary_keys(tables)
        primary_key_by_table = result.primary_key_by_table
        warnings = list(result.warnings)
        return primary_key_by_table, warnings
