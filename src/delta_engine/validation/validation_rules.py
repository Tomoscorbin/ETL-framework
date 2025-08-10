"""
Validation rules for schema table plans.

This module defines a validation framework for `Plan` objects, allowing
rules to enforce safety constraints before execution. Each rule inspects
the plan and raises `UnsafePlanError` if violations are found.
"""

from __future__ import annotations
from typing import Sequence, Tuple
import pyspark.sql.types as T

from src.delta_engine.actions import Plan
from src.delta_engine.models import Table, Column
from src.delta_engine.constraints.naming import build_primary_key_name, build_foreign_key_name


# ----------------- EXCEPTIONS -----------------

class UnsafePlanError(Exception):
    """Plan violates safety rules."""

class PrimaryKeyError(Exception):
    """Primary key constraint is invalid."""

class ForeignKeyError(Exception):
    """Foreign key constraint is invalid."""

class ConstraintNameCollision(Exception):
    """Constraint name collides with another constraint name."""


# ----------------- VALIDATION RULE -----------------

class ValidationRule:
    """Base interface for a plan validation rule."""

    def check(self, plan: Plan) -> None:
        """Validate the given plan against the rule."""
        raise NotImplementedError


# ----------------- DDL -----------------

class NoAddNotNullColumnsRule(ValidationRule):
    """
    For AlignTable actions, never plan ADD COLUMN with is_nullable=False.
    Columns should be added as NULLABLE, then tightened after backfill.
    This check safeguards against not nullable columns being
    added to pre-existing tables, where new columns are automatically
    populated with nulls upon creation.
    """

    def check(self, plan: Plan) -> None:
        """Validate that no AlignTable action adds a NOT NULL column."""
        for action in plan.align_tables:
            for add in action.add_columns:
                if add.is_nullable is False:
                    full_name = f"{action.catalog_name}.{action.schema_name}.{action.table_name}"
                    raise UnsafePlanError(
                        f"Unsafe plan: ADD NOT NULL column `{add.name}` on {full_name}. "
                        "Add as NULLABLE first, backfill in ETL, then tighten nullability later."
                    )



# ----------------- CONSTRAINT RULES -----------------

# ----------------- helpers -----------------

def three_part_name(table: Table) -> Tuple[str, str, str]:
    return (table.catalog_name, table.schema_name, table.table_name)

def build_table_index(tables: Sequence[Table]) -> dict[Tuple[str, str, str], Table]:
    return {three_part_name(t): t for t in tables}

def column_index(table: Table) -> dict[str, Column]:
    return {c.name: c for c in table.columns}

def is_complex_type(dt: T.DataType) -> bool:
    return isinstance(dt, (T.ArrayType, T.MapType, T.StructType))

def type_string(dt: T.DataType) -> str:
    return dt.simpleString().upper()

def types_match_exactly(left: T.DataType, right: T.DataType) -> bool:
    return type_string(left) == type_string(right)

# ----------------- PK rules -----------------

class PrimaryKeyNonEmptyRule(ValidationRule):
    def check(self, tables: Sequence[Table]) -> None:
        for t in tables:
            if t.primary_key is not None and len(t.primary_key) == 0:
                raise PrimaryKeyError(f"{three_part_name(t)}: primary_key is empty.")

class PrimaryKeyNoDuplicatesRule(ValidationRule):
    def check(self, tables: Sequence[Table]) -> None:
        for t in tables:
            if not t.primary_key:
                continue
            names = list(t.primary_key)
            if len(set(names)) != len(names):
                raise PrimaryKeyError(f"{three_part_name(t)}: primary_key has duplicate columns: {names}.")

class PrimaryKeyColumnsExistRule(ValidationRule):
    def check(self, tables: Sequence[Table]) -> None:
        for t in tables:
            if not t.primary_key:
                continue
            cols = column_index(t)
            missing = [n for n in t.primary_key if n not in cols]
            if missing:
                raise PrimaryKeyError(f"{three_part_name(t)}: primary_key references missing column(s): {missing}.")

class PrimaryKeyColumnsNotNullableRule(ValidationRule):
    def check(self, tables: Sequence[Table]) -> None:
        for t in tables:
            if not t.primary_key:
                continue
            cols = column_index(t)
            nullable = [n for n in t.primary_key if n in cols and cols[n].is_nullable]
            if nullable:
                raise PrimaryKeyError(f"{three_part_name(t)}: primary_key column(s) must be NOT NULL: {nullable}.")

class PrimaryKeyNoComplexTypesRule(ValidationRule):
    def check(self, tables: Sequence[Table]) -> None:
        for t in tables:
            if not t.primary_key:
                continue
            cols = column_index(t)
            complex_cols = [
                f"{n}({type_string(cols[n].data_type)})"
                for n in t.primary_key if n in cols and is_complex_type(cols[n].data_type)
            ]
            if complex_cols:
                raise PrimaryKeyError(f"{three_part_name(t)}: primary_key uses unsupported complex type(s): {complex_cols}.")

# ----------------- FK rules -----------------

class ForeignKeyTargetTableExistsRule(ValidationRule):
    def check(self, tables: Sequence[Table]) -> None:
        idx = build_table_index(tables)
        for src in tables:
            for fk in src.foreign_keys:
                if fk.target_table not in idx:
                    raise ForeignKeyError(f"{three_part_name(src)}: foreign key targets unknown table {fk.target_table}.")

class ForeignKeyTargetHasPrimaryKeyRule(ValidationRule):
    def check(self, tables: Sequence[Table]) -> None:
        idx = build_table_index(tables)
        for src in tables:
            for fk in src.foreign_keys:
                tgt = idx.get(fk.target_table)
                if tgt and not tgt.primary_key:
                    raise ForeignKeyError(f"{three_part_name(src)}: foreign key targets {fk.target_table} which has no primary_key.")

class ForeignKeyTargetColumnsMatchPrimaryKeyRule(ValidationRule):
    def check(self, tables: Sequence[Table]) -> None:
        idx = build_table_index(tables)
        for src in tables:
            for fk in src.foreign_keys:
                tgt = idx.get(fk.target_table)
                if not tgt or not tgt.primary_key:
                    continue
                if list(fk.target_columns) != list(tgt.primary_key):
                    raise ForeignKeyError(
                        f"{three_part_name(src)}: foreign key target_columns {list(fk.target_columns)} "
                        f"must match target primary_key {list(tgt.primary_key)} exactly."
                    )

class ForeignKeyArityMatchesRule(ValidationRule):
    def check(self, tables: Sequence[Table]) -> None:
        for src in tables:
            for fk in src.foreign_keys:
                if len(fk.source_columns) != len(fk.target_columns):
                    raise ForeignKeyError(
                        f"{three_part_name(src)}: foreign key arity mismatch; "
                        f"source={list(fk.source_columns)}, target={list(fk.target_columns)}."
                    )

class ForeignKeySourceColumnsExistRule(ValidationRule):
    def check(self, tables: Sequence[Table]) -> None:
        for src in tables:
            cols = column_index(src)
            for fk in src.foreign_keys:
                missing = [n for n in fk.source_columns if n not in cols]
                if missing:
                    raise ForeignKeyError(f"{three_part_name(src)}: foreign key source column(s) do not exist: {missing}.")

class ForeignKeyTargetColumnsExistRule(ValidationRule):
    def check(self, tables: Sequence[Table]) -> None:
        idx = build_table_index(tables)
        for src in tables:
            for fk in src.foreign_keys:
                tgt = idx.get(fk.target_table)
                if not tgt:
                    continue
                tgt_cols = column_index(tgt)
                missing = [n for n in fk.target_columns if n not in tgt_cols]
                if missing:
                    raise ForeignKeyError(f"{three_part_name(src)}: foreign key target column(s) missing on {fk.target_table}: {missing}.")

class ForeignKeySourceColumnsNotNullableRule(ValidationRule):
    def check(self, tables: Sequence[Table]) -> None:
        for src in tables:
            cols = column_index(src)
            for fk in src.foreign_keys:
                nullable = [n for n in fk.source_columns if n in cols and cols[n].is_nullable]
                if nullable:
                    raise ForeignKeyError(f"{three_part_name(src)}: foreign key source column(s) must be NOT NULL: {nullable}.")

class ForeignKeyTypesMatchExactlyRule(ValidationRule):
    def check(self, tables: Sequence[Table]) -> None:
        idx = build_table_index(tables)
        for src in tables:
            src_cols = column_index(src)
            for fk in src.foreign_keys:
                tgt = idx.get(fk.target_table)
                if not tgt:
                    continue
                tgt_cols = column_index(tgt)
                mismatches = []
                for s_name, t_name in zip(fk.source_columns, fk.target_columns):
                    if s_name in src_cols and t_name in tgt_cols:
                        if not types_match_exactly(src_cols[s_name].data_type, tgt_cols[t_name].data_type):
                            mismatches.append(
                                f"'{s_name}'({type_string(src_cols[s_name].data_type)}) -> "
                                f"'{t_name}'({type_string(tgt_cols[t_name].data_type)})"
                            )
                if mismatches:
                    raise ForeignKeyError(f"{three_part_name(src)}: type mismatch in foreign key mapping(s): {mismatches}.")

# ----------------- name collisions -----------------

class ConstraintNameCollisionRule(ValidationRule):
    def check(self, tables: Sequence[Table]) -> None:
        pk_names: dict[str, list[str]] = {}
        fk_names: dict[str, list[str]] = {}

        for t in tables:
            if t.primary_key:
                name = build_pk_name(three_part_name(t), t.primary_key)
                pk_names.setdefault(name, []).append(str(three_part_name(t)))

        for src in tables:
            src_three = three_part_name(src)
            for fk in src.foreign_keys:
                name = build_fk_name(
                    source_three_part_table_name=src_three,
                    source_columns=fk.source_columns,
                    target_three_part_table_name=fk.target_table,
                    target_columns=fk.target_columns,
                )
                ctx = f"{src_three} -> {fk.target_table} ({list(fk.source_columns)} -> {list(fk.target_columns)})"
                fk_names.setdefault(name, []).append(ctx)

        dup_pks = {n: ctx for n, ctx in pk_names.items() if len(ctx) > 1}
        dup_fks = {n: ctx for n, ctx in fk_names.items() if len(ctx) > 1}

        if dup_pks:
            [(n, ctx)] = dup_pks.items()
            raise ConstraintNameCollision(f"Duplicate PK name '{n}' across: " + "; ".join(ctx))
        if dup_fks:
            [(n, ctx)] = dup_fks.items()
            raise ConstraintNameCollision(f"Duplicate FK name '{n}' across: " + "; ".join(ctx))
