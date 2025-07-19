import os 
from typing import Final
from pyspark.sql import SparkSession

from src.enums import Catalog

_catalog = os.getenv(key="CATALOG", default="dev")
 
spark = SparkSession.builder.appName("ETL-DEMO").getOrCreate()
CATALOG: Final[str] = Catalog(_catalog)