"""Utility functions used across models (identifiers, names, SQL safety)."""


def build_foreign_key_name(
    source_catalog: str,
    source_schema: str,
    source_table: str,
    source_column: str,
    target_table: str,
    target_column: str,
) -> str:
    """Construct a deterministic foreign key constraint name from source and target details."""
    return (
        "fk"
        f"_{source_catalog}"
        f"_{source_schema}"
        f"_{source_table}"
        f"_{source_column}"
        f"_to_{target_table}_{target_column}"
    )
