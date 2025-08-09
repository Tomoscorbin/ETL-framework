from dataclasses import dataclass
from typing import Optional
from pyspark.sql import SparkSession
from src.delta_engine.actions import Plan


class UnsafePlanError(Exception):
    """
    Raised when a generated plan contains operations that
    violate safety rules (e.g., adding NOT NULL columns to a populated table).
    """
    pass


@dataclass
class PlanValidator:
    """
    Post-plan validation. Raise RuntimeError on violations.
    - Rule 1: For AlignTable actions, forbid planning ADD COLUMN with is_nullable=False.
              (Optionally allow if the table is empty.)
    """

    def validate(self, plan: Plan, spark: SparkSession) -> None:
        for action in plan.align_tables:
            full_name = f"{action.catalog_name}.{action.schema_name}.{action.table_name}"

            # Rule 1: New columns must be added as nullable (DDL-safe).
            for add in action.add_columns:
                if add.is_nullable:
                    continue

                if self._is_table_empty(spark, full_name):
                    # Allowed when empty; executor will add column (nullable) then set NOT NULL
                    continue

                raise UnsafePlanError(
                    f"Unsafe plan: attempting to ADD NOT NULL column `{add.name}` on {full_name}."
                    " Add new columns as nullable, then backfill in ETL, then tighten nullability in a later run."
                )

    # ---- helpers ----
    def _is_table_empty(self, spark: SparkSession, full_name: str) -> bool:
        # Cheap emptiness check without scanning the whole table.
        try:
            return spark.table(full_name).isEmpty()
        except Exception:
            # If the table somehow doesn't exist yet, treat as empty.
            return True
