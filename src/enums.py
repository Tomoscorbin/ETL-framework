from enum import StrEnum


class Catalog(StrEnum):
    """Catalog name in Unity Catalog."""

    DEV = "dev"
    PROD = "prod"


class Medallion(StrEnum):
    """Layer in the medallion architecture."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    METADATA = "metadata"


class DeltaTableProperty(StrEnum):
    """Delta Table properties."""

    COLUMN_MAPPING_MODE = "delta.columnMapping.mode"
