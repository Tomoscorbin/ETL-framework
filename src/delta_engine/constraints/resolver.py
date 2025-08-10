from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Iterable

from src.delta_engine.models import Table, ForeignKey as ForeignKeyModel
from src.delta_engine.constraints.specs import PrimaryKeySpec, ForeignKeySpec
from src.delta_engine.constraints.naming import build_primary_key_name, build_foreign_key_name


@dataclass(frozen=True)
class ResolvedConstraints:
    primary_keys: tuple[PrimaryKeySpec, ...]
    foreign_keys: tuple[ForeignKeySpec, ...]


# ---------- public API ----------

def resolve_from_models(tables: Sequence[Table]) -> ResolvedConstraints:
    """
    Convert Table models into fully specified PK/FK specs.
    - Fills FK target columns from the target table's PK when omitted.
    - Computes deterministic names when not provided.
    - Does not perform structural validation (that is the validator's job).
    """
    table_index = _build_table_index(tables)

    primary_keys = _resolve_primary_keys(tables)
    foreign_keys = _resolve_all_foreign_keys(tables, table_index)

    return ResolvedConstraints(
        primary_keys=tuple(primary_keys),
        foreign_keys=tuple(foreign_keys),
    )


# ---------- helpers: table addressing ----------

def _three_part_name(table: Table) -> tuple[str, str, str]:
    """Return (catalog, schema, table) for the given model."""
    return (table.catalog_name, table.schema_name, table.table_name)


def _build_table_index(tables: Sequence[Table]) -> dict[tuple[str, str, str], Table]:
    """Allow O(1) lookup of a table model by its three-part name."""
    return {_three_part_name(t): t for t in tables}


# ---------- helpers: primary keys ----------

def _resolve_primary_keys(tables: Sequence[Table]) -> list[PrimaryKeySpec]:
    """Extract PrimaryKeySpec objects from models that declare a primary key."""
    specs: list[PrimaryKeySpec] = []
    for table in tables:
        if not table.primary_key:
            continue
        three_part = _three_part_name(table)
        name = build_primary_key_name(three_part, table.primary_key)
        specs.append(
            PrimaryKeySpec(
                three_part_table_name=three_part,
                columns=tuple(table.primary_key),
                name=name,
            )
        )
    return specs


# ---------- helpers: foreign keys ----------

def _resolve_all_foreign_keys(
    tables: Sequence[Table],
    table_index: dict[tuple[str, str, str], Table],
) -> list[ForeignKeySpec]:
    """Resolve foreign keys for every table."""
    all_specs: list[ForeignKeySpec] = []
    for table in tables:
        specs_for_table = _resolve_foreign_keys_for_table(
            source_table=table, table_index=table_index
        )
        all_specs.extend(specs_for_table)
    return all_specs


def _resolve_foreign_keys_for_table(
    source_table: Table,
    table_index: dict[tuple[str, str, str], Table],
) -> list[ForeignKeySpec]:
    """Resolve all FKs declared on a single source table."""
    specs: list[ForeignKeySpec] = []
    source_three_part = _three_part_name(source_table)

    for fk_model in source_table.foreign_keys:
        target_three_part = fk_model.target_table
        target_table = table_index.get(target_three_part)

        target_columns = _choose_target_columns(fk_model, target_table)
        fk_name = _choose_foreign_key_name(
            fk_model=fk_model,
            source_three_part_name=source_three_part,
            target_three_part_name=target_three_part,
            target_columns=target_columns,
        )

        specs.append(
            ForeignKeySpec(
                source_three_part_table_name=source_three_part,
                source_columns=tuple(fk_model.source_columns),
                target_three_part_table_name=target_three_part,
                target_columns=tuple(target_columns),
                name=fk_name,
            )
        )
    return specs


def _choose_target_columns(
    fk_model: ForeignKeyModel,
    target_table: Table | None,
) -> tuple[str, ...]:
    """Use explicitly provided target columns; no inference."""
    return tuple(fk_model.target_columns)


def _choose_foreign_key_name(
    fk_model: ForeignKeyModel,
    source_three_part_name: tuple[str, str, str],
    target_three_part_name: tuple[str, str, str],
    target_columns: Iterable[str],
) -> str:
    return build_foreign_key_name(
        source_three_part_table_name=source_three_part_name,
        source_columns=fk_model.source_columns,
        target_three_part_table_name=target_three_part_name,
        target_columns=target_columns,
    )
