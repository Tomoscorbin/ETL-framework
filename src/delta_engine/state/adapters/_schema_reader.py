# src/delta_engine/state/adapters/_schema_reader.py
from __future__ import annotations

from typing import NamedTuple
import pyspark.sql.types as T
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from src.delta_engine.state.ports import SnapshotWarning, Aspect
from src.delta_engine.identifiers import TableIdentity
from src.delta_engine.state.states import ColumnState
from src.delta_engine.utils import qualify_table_name


class SchemaReadResult(NamedTuple):
    """Result of attempting to load a single table's schema."""
    exists: bool
    struct: T.StructType | None
    warning: SnapshotWarning | None


class SchemaBatchReadResult(NamedTuple):
    """Aggregated result for attempting to load multiple table schemas."""
    existence_by_table: dict[TableIdentity, bool]
    schema_by_table: dict[TableIdentity, tuple[ColumnState, ...]]
    warnings: list[SnapshotWarning]


class SchemaReader:
    """
    Determine table existence and (optionally) load the physical schema
    (column name, data type, nullability) for a list of tables.

    - No comments, properties, or constraints here.
    - No TableState construction here; just raw facts for the builder.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def read_many(
        self,
        tables: tuple[TableIdentity, ...],
        include_schema: bool,
    ) -> SchemaBatchReadResult:
        existence_by_table: dict[TableIdentity, bool] = {}
        schema_by_table: dict[TableIdentity, tuple[ColumnState, ...]] = {}
        warnings: list[SnapshotWarning] = []

        for identity in tables:
            single_result = self._read_single(identity, include_schema)

            existence_by_table[identity] = single_result.exists

            if single_result.exists and single_result.struct is not None:
                schema_by_table[identity] = self._struct_to_column_states(single_result.struct)
            else:
                schema_by_table[identity] = tuple()

            if single_result.warning is not None:
                warnings.append(single_result.warning)

        return SchemaBatchReadResult(
            existence_by_table=existence_by_table,
            schema_by_table=schema_by_table,
            warnings=warnings,
        )

    # ---------- helpers ----------

    def _read_single(self, identity: TableIdentity, include_schema: bool) -> SchemaReadResult:
        """Check existence, and if requested, load StructType. Produce warnings on failures."""
        qualified_name = qualify_table_name(identity)

        exists, existence_warning = self._check_existence(qualified_name, identity)
        if existence_warning is not None:
            return SchemaReadResult(exists=False, struct=None, warning=existence_warning)

        if not exists:
            return SchemaReadResult(exists=False, struct=None, warning=None)

        if not include_schema:
            return SchemaReadResult(exists=True, struct=None, warning=None)

        struct, schema_warning = self._load_struct(qualified_name, identity)
        if schema_warning is not None:
            return SchemaReadResult(exists=True, struct=None, warning=schema_warning)

        return SchemaReadResult(exists=True, struct=struct, warning=None)

    # ---------- tiny helpers ----------

    def _check_existence(
        self,
        qualified_name: str,
        identity: TableIdentity,
    ) -> tuple[bool, SnapshotWarning | None]:
        """Return (exists, warning). If the check itself fails, warn and treat as not existing."""
        try:
            exists = self.spark.catalog.tableExists(qualified_name)
            return exists, None
        
        except Exception as error:
            error_formatted = self._format_exception_brief(error)
            warning = SnapshotWarning(
                identity,
                Aspect.SCHEMA,
                f"Failed to check existence: {error_formatted}",
            )
            return False, warning

    def _load_struct(
        self,
        qualified_name: str,
        identity: TableIdentity,
    ) -> tuple[T.StructType | None, SnapshotWarning | None]:
        """Return (struct, warning). If loading fails (e.g., permissions), warn and return None."""
        try:
            delta_table = DeltaTable.forName(self.spark, qualified_name)
            struct = delta_table.toDF().schema
            return struct, None
        except Exception as error:
            error_formatted = self._format_exception_brief(error)
            warning = SnapshotWarning(
                identity,
                Aspect.SCHEMA,
                f"Failed to load schema: {error_formatted}",
            )
            return None, warning

    @staticmethod
    def _struct_to_column_states(struct: T.StructType) -> tuple[ColumnState, ...]:
        """Map Spark fields to immutable ColumnState objects (comments merged later)."""
        return tuple(
            ColumnState(
                name=field.name,
                data_type=field.dataType,
                is_nullable=field.nullable,
                comment="",  # comment merge happens in the builder
            )
            for field in struct.fields
        )

    def _format_exception_brief(error: Exception) -> str:
        """One-line, short message for SnapshotWarning."""
        exception_name = type(error).__name__
        first_line = str(error).strip().splitlines()[0]
        brief = f"{exception_name}: {first_line}"
        return brief[:300]  # cap to avoid giant warnings
