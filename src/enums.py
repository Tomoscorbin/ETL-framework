from enum import StrEnum

class Catalog(StrEnum):
    "Catalog name in Unity Catalog."

    DEV = "dev"
    PROD = "prod"

class DeltaTableProperty(StrEnum):
    """Delta Table properties."""
    
    COLUMN_MAPPING_MODE = "delta.columnMapping.mode"
