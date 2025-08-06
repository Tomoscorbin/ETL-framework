"""Configuration values sourced from environment variables."""

import os
from typing import Final

from src.enums import Catalog, SourceCatalog, SourceSchema

_catalog = os.getenv(key="CATALOG", default="dev")
_source_catalog = os.getenv("SOURCE_CATALOG", default="source")
_source_schema = os.getenv("SOURCE_SCHEMA", default="raw")


CATALOG: Final[str] = Catalog(_catalog)
SOURCE_CATALOG: Final[str] = SourceCatalog(_source_catalog)
SOURCE_SCHEMA: Final[str] = SourceSchema(_source_schema)
LOG_LEVEL: Final[str] = os.getenv(key="LOG_LEVEL", default="INFO")
LOGGER_NAME: Final[str] = os.getenv(key="LOGGER_NAME", default="etl-framework")
LOG_COLOUR_ENABLED: Final[bool] = bool(
    os.getenv(key="LOG_COLOUR_ENABLED", default="True").upper() == "TRUE"
)
