from dataclasses import dataclass


@dataclass(frozen=True)
class PrimaryKeySpec:
    three_part_table_name: tuple[str, str, str]
    columns: tuple[str, ...]
    name: str

@dataclass(frozen=True)
class ForeignKeySpec:
    source_three_part_table_name: tuple[str, str, str]
    source_columns: tuple[str, ...]
    target_three_part_table_name: tuple[str, str, str]
    target_columns: tuple[str, ...]
    name: str
