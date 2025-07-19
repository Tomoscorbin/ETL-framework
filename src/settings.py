import os
from typing import Final

from pyspark.sql import SparkSession

from src.enums import Catalog

_catalog = os.getenv(key="CATALOG", default="dev")

spark = SparkSession.builder.appName("ETL-DEMO").getOrCreate()
CATALOG: Final[str] = Catalog(_catalog)
LOG_LEVEL: Final[str] = os.getenv(key="LOG_LEVEL", default="INFO")
LOGGER_NAME: Final[str] = os.getenv(key="LOGGER_NAME", default="etl-framework")
LOG_COLOUR_ENABLED: Final[bool] = bool(
    os.getenv(key="LOG_COLOUR_ENABLED", default="True").upper() == "TRUE"
)
