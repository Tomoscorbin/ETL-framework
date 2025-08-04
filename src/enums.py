from enum import StrEnum


class Catalog(StrEnum):
    """Catalog name in Unity Catalog."""

    DEV = "dev"
    PROD = "prod"


class SourceCatalog(StrEnum):
    """Source catalog name in Unity Catalog."""

    SOURCE = "source"


class SourceSchema(StrEnum):
    """Source schema name in Unity Catalog."""

    RAW = "raw"


class Medallion(StrEnum):
    """Layer in the medallion architecture."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    METADATA = "metadata"


class DeltaTableProperty(StrEnum):
    """Delta Table properties."""

    COLUMN_MAPPING_MODE = "delta.columnMapping.mode"


class DQFailureSeverity(StrEnum):
    """Failure severity for DQX."""

    ERROR = "error"
    WARNING = "warning"


class ResultState(StrEnum):
    """Databricks job run result state."""

    SUCCEEDED = "SUCCEEDED"
    ERROR = "ERROR"
