from src.delta_engine.actions import Plan

class ValidationRule:
    """Base interface for a plan validation rule."""
    def check(self, plan: Plan) -> None:
        raise NotImplementedError

class NoAddNotNullColumnsRule(ValidationRule):
    """
    For AlignTable actions, never plan ADD COLUMN with is_nullable=False.
    Columns should be added as NULLABLE, then tightened after backfill.
    """
    def check(self, plan: Plan) -> None:
        for action in plan.align_tables:
            for add in action.add_columns:
                if add.is_nullable is False:
                    full_name = f"{action.catalog_name}.{action.schema_name}.{action.table_name}"
                    raise UnsafePlanError(
                        f"Unsafe plan: ADD NOT NULL column `{add.name}` on {full_name}. "
                        "Add as NULLABLE first, backfill in ETL, then tighten nullability later."
                    )

class UnsafePlanError(Exception):
    """Plan violates safety rules."""