from __future__ import annotations
from typing import List
from src.delta_engine.constraints.actions import (
    ConstraintPlan, CreatePrimaryKey, CreateForeignKey
)
from src.delta_engine.constraints.naming import three_part_to_qualified_name

def render_plan(plan: ConstraintPlan) -> List[str]:
    """Render a full plan to ordered SQL statements."""
    statements: list[str] = []
    for action in plan.create_primary_keys:
        statements.append(render_add_primary_key(action))
    for action in plan.create_foreign_keys:
        statements.append(render_add_foreign_key(action))
    return statements

def render_add_primary_key(action: CreatePrimaryKey) -> str:
    """ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (...);"""
    tbl = three_part_to_qualified_name(action.three_part_table_name)
    cols = ", ".join(f"`{c}`" for c in action.columns)
    return f"ALTER TABLE {tbl} ADD CONSTRAINT `{action.name}` PRIMARY KEY ({cols});"

def render_add_foreign_key(action: CreateForeignKey) -> str:
    """ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY (...) REFERENCES ... (...);"""
    src = three_part_to_qualified_name(action.source_three_part_table_name)
    tgt = three_part_to_qualified_name(action.target_three_part_table_name)
    src_cols = ", ".join(f"`{c}`" for c in action.source_columns)
    tgt_cols = ", ".join(f"`{c}`" for c in action.target_columns)
    return (
        f"ALTER TABLE {src} ADD CONSTRAINT `{action.name}` "
        f"FOREIGN KEY ({src_cols}) REFERENCES {tgt} ({tgt_cols});"
    )
