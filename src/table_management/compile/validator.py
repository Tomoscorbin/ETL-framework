# src/table_management/compile/validate.py
from dataclasses import dataclass
from typing import Sequence

@dataclass(frozen=True)
class PreflightValidator:
    def validate_align(
        self,
        full_name: str,
        desired_pk_cols: Sequence[str],
        actual_pk_cols: Sequence[str],
        add_column_names: Sequence[str],
        drop_column_names: Sequence[str],
        will_drop_pk: bool,
    ) -> None:
        # 1) forbid PK on newly-added columns (DDL-only)
        bad = sorted(set(desired_pk_cols).intersection(add_column_names))
        if bad:
            raise RuntimeError(
                f"Cannot set PRIMARY KEY on {full_name}. Column(s) {bad} are newly added and NULL for existing rows. "
                "DDL-only flow forbids backfill. Add column, backfill externally, then rerun."
            )

        # 2) forbid dropping a PK member unless PK is being dropped in the plan
        hits = sorted(set(actual_pk_cols).intersection(drop_column_names))
        if hits and not will_drop_pk:
            raise RuntimeError(
                f"Cannot drop column(s) {hits} on {full_name} because they are part of the current PRIMARY KEY. "
                "Drop the PK first."
            )
