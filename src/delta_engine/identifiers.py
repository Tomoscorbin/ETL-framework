from dataclasses import dataclass

@dataclass(frozen=True)
class TableIdentity:
    catalog: str
    schema: str
    table: str
