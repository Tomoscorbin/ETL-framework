import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[1]))

from pyspark.sql import SparkSession

import src
from src import settings
from src.ddl.utils import ensure_all_delta_tables, ensure_all_schemas
from src.enums import Medallion


def run_ddl(spark: SparkSession) -> None:
    ensure_all_schemas(
        schema_names=Medallion,
        catalog_name=settings.CATALOG,
        spark=spark
    )
    ensure_all_delta_tables(
        package=src,
        spark=spark
    )


if __name__ == "__main__":
    from src.settings import spark
    run_ddl(spark)