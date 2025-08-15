"""
Builder: assemble TableState/CatalogState from independently-read slices.

Inputs (per table)
------------------
- existence flag
- physical columns (ColumnState...)   [schema reader]
- per-column comments {lower_name -> str}   [column comments reader]
- table comment str                        [table comment reader]
- table properties {str: str}              [properties reader]
- primary key (PrimaryKeyState | None)     [constraints reader]

Responsibilities
----------------
- Merge column comments into ColumnState (case-insensitive by column name).
- Provide sensible defaults when slices are missing.
- Produce a deterministic CatalogState keyed by unescaped 'catalog.schema.table'.
"""

from __future__ import annotations

from collections.abc import Mapping

from src.delta_engine.identifiers import FullyQualifiedTableName, fully_qualified_name_to_string
from src.delta_engine.state.states import CatalogState, ColumnState, PrimaryKeyState, TableState


class TableStateBuilder:
    """Pure assembler: no I/O, no Spark — just combine slices into state objects."""

    def assemble(
        self,
        *,
        tables: tuple[FullyQualifiedTableName, ...],
        exists: Mapping[FullyQualifiedTableName, bool],
        schema: Mapping[FullyQualifiedTableName, tuple[ColumnState, ...]],
        column_comments: Mapping[FullyQualifiedTableName, Mapping[str, str]],
        table_comments: Mapping[FullyQualifiedTableName, str],
        properties: Mapping[FullyQualifiedTableName, Mapping[str, str]],
        primary_keys: Mapping[FullyQualifiedTableName, PrimaryKeyState | None],
    ) -> CatalogState:
        """
        Create a CatalogState covering all `tables`. Missing slice entries default to:
          - exists=False
          - columns=()
          - column comments={}
          - table_comment=""
          - properties={}
          - primary_key=None
        """
        tables_dict: dict[str, TableState] = {}

        for name in tables:
            exists_flag = bool(exists.get(name, False))
            columns_in = tuple(schema.get(name, ()))
            comments_in = dict(column_comments.get(name, {}))  # lowercased keys expected
            table_comment_in = str(table_comments.get(name, "") or "")
            properties_in = dict(properties.get(name, {}))
            primary_key_in = primary_keys.get(name, None)

            merged_columns = self._merge_column_comments(columns_in, comments_in)

            table_state = TableState(
                catalog_name=name.catalog,
                schema_name=name.schema,
                table_name=name.table,
                exists=exists_flag,
                columns=merged_columns,
                table_comment=table_comment_in,
                table_properties=properties_in,
                primary_key=primary_key_in,
            )

            full_name = fully_qualified_name_to_string(name)
            tables_dict[full_name] = table_state

        return CatalogState(tables=tables_dict)

    # ---------- tiny helpers ----------

    @staticmethod
    def _merge_column_comments(
        columns: tuple[ColumnState, ...],
        comments_by_lower_name: Mapping[str, str],
    ) -> tuple[ColumnState, ...]:
        """
        Overlay comments onto ColumnState by case-insensitive name.
        Empty or missing comments are treated as "".
        """
        if not columns:
            return tuple()

        out: list[ColumnState] = []
        for col in columns:
            lookup_key = col.name.lower()
            comment_value = comments_by_lower_name.get(lookup_key, "")
            if comment_value is None:
                comment_value = ""
            # ColumnState is immutable; create a new one with the comment applied.
            out.append(
                ColumnState(
                    name=col.name,
                    data_type=col.data_type,
                    is_nullable=col.is_nullable,
                    comment=str(comment_value),
                )
            )
        return tuple(out)
