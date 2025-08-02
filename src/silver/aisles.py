import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[1]))

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

from src import settings
from src.enums import Medallion
from src.models.column import DeltaColumn
from src.models.table import DeltaTable

aisles = DeltaTable(
    table_name="aisles",
    schema_name=Medallion.SILVER,
    catalog_name=settings.CATALOG,
    columns=[
        DeltaColumn(
            name="aisle_id",
            data_type=T.IntegerType(),
            is_primary_key=True,
            is_nullable=False,
            comment="Unique identifier for an aisle",
        ),
        DeltaColumn(
            name="aisle_name",
            data_type=T.StringType(),
            is_nullable=False,
            comment="Name of the aisle",
        ),
    ],
)


def main(spark: SparkSession) -> None:
    """Execute the pipeline."""
    raw_aisles_df = spark.table(f"{settings.CATALOG}.{Medallion.BRONZE}.aisles")
    aisles_cleaned_df = raw_aisles_df.select(
        F.col("aisle_id").cast(T.IntegerType()).alias("aisle_id"),
        F.col("aisle").alias("aisle_name"),
    )

    aisles.overwrite(aisles_cleaned_df)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark)
