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

products = DeltaTable(
    table_name="products",
    schema_name=Medallion.SILVER,
    catalog_name=settings.CATALOG,
    columns=[
        DeltaColumn(
            name="product_id",
            data_type=T.IntegerType(),
            is_primary_key=True,
            is_nullable=False,
        ),
        DeltaColumn(name="product_name", data_type=T.StringType(), is_nullable=False),
        DeltaColumn(name="aisle_id", data_type=T.IntegerType(), is_nullable=False),
        DeltaColumn(name="department_id", data_type=T.IntegerType(), is_nullable=False),
    ],
)


def main(spark: SparkSession) -> None:
    """Execute the pipeline."""
    raw_products_df = spark.table(f"{settings.CATALOG}.{Medallion.BRONZE}.products")
    products_cleaned_df = raw_products_df.select(
        F.col("product_id").cast(T.IntegerType()).alias("product_id"),
        F.col("product_name").alias("product_name"),
        F.col("aisle_id").cast(T.IntegerType()).alias("aisle_id"),
        F.col("department_id").cast(T.IntegerType()).alias("department_id"),
    )

    products.overwrite(products_cleaned_df)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark)
