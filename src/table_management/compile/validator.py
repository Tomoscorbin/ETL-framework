from dataclasses import dataclass
from typing import Sequence
from src.table_management.state.snapshot import CatalogState
from src.table_management.models import ForeignKey


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
                f"Cannot set PRIMARY KEY on {full_name}. Column(s) {bad} are newly added and NULL for existing rows."
                " Add column, backfill externally, then rerun."
            )

        # 2) forbid dropping a PK member unless PK is being dropped in the plan
        hits = sorted(set(actual_pk_cols).intersection(drop_column_names))
        if hits and not will_drop_pk:
            raise RuntimeError(
                f"Cannot drop column(s) {hits} on {full_name} because they are part of the current PRIMARY KEY."
                " Drop the PK first."
            )

    def validate_foreign_keys(
        self,
        catalog_state: CatalogState,
        catalog: str, schema: str, src_table: str,
        desired_fks: Sequence[ForeignKey],
        add_column_names: Sequence[str],
    ) -> None:
        full_name = f"{catalog}.{schema}.{src_table}"
        adding = set(add_column_names)

        for fk in desired_fks:
            if len(fk.source_columns) != len(fk.reference_columns):
                raise RuntimeError(f"{full_name}: FK must map 1:1 columns: {fk}")

            # no FK if any source col is being added now
            new_hits = sorted(set(fk.source_columns).intersection(adding))
            if new_hits:
                raise RuntimeError(
                    f"{full_name}: Cannot add FK using newly added column(s) {new_hits}. "
                    "DDL-only flow forbids backfill; add/backfill first, then add FK."
                )

            # type check (fast-fail)
            src_state = catalog_state.get(catalog, schema, src_table)
            ref_state = catalog_state.get(catalog, schema, fk.reference_table_name)
            if src_state is None or ref_state is None:
                raise RuntimeError(f"{full_name}: FK references unknown table(s).")
            src_types = {c.name: c.data_type for c in src_state.columns}
            ref_types = {c.name: c.data_type for c in ref_state.columns}
            for s, r in zip(fk.source_columns, fk.reference_columns):
                if src_types.get(s) is None or ref_types.get(r) is None:
                    raise RuntimeError(f"{full_name}: FK references unknown column(s) {s}->{r}.")
                if (getattr(src_types[s], "simpleString", lambda: str(src_types[s]))()
                    != getattr(ref_types[r], "simpleString", lambda: str(ref_types[r]))()):
                    raise RuntimeError(f"{full_name}: FK type mismatch {s} vs {fk.reference_table_name}.{r}.")