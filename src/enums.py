"""Enumerations used throughout the ETL framework."""

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


class ResultState(StrEnum):
    """Databricks job run result state."""

    SUCCEEDED = "SUCCEEDED"
    ERROR = "ERROR"


class DQCriticality(StrEnum):
    """Data quality failure criticality."""

    ERROR = "error"
    WARN = "warn"

    @property
    def quarantine_column(self) -> str:
        """Failure column names for DQX quarantine table."""
        mapping = {
            DQCriticality.ERROR: "_errors",
            DQCriticality.WARN: "_warnings",
        }
        return mapping[self]
