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

product_dimension = DeltaTable(
    table_name="dim_product",
    schema_name=Medallion.GOLD,
    catalog_name=settings.CATALOG,
    columns=[
        DeltaColumn(
            name="product_id",
            data_type=T.IntegerType(),
            is_primary_key=True,
            is_nullable=False,
            comment="Identifier for the product",
        ),
        DeltaColumn(
            name="product_name",
            data_type=T.StringType(),
            is_nullable=False,
            comment="Name of the product",
        ),
        DeltaColumn(
            name="aisle",
            data_type=T.StringType(),
            is_nullable=False,
            comment="Name of the aisle containing the product",
        ),
        DeltaColumn(
            name="department",
            data_type=T.StringType(),
            is_nullable=False,
            comment="Name of the department containing the product",
        ),
    ],
)


def main(spark: SparkSession) -> None:
    """Build the product dimension."""
    product_table = f"{settings.CATALOG}.{Medallion.SILVER}.product"
    aisle_table = f"{settings.CATALOG}.{Medallion.SILVER}.aisle"
    department_table = f"{settings.CATALOG}.{Medallion.SILVER}.department"

    products_df = spark.table(product_table)
    aisles_df = spark.table(aisle_table)
    departments_df = spark.table(department_table)

    product_dim_df = (
        products_df.join(aisles_df, "aisle_id")
        .join(departments_df, "department_id")
        .select(
            F.col("product_id").cast(T.IntegerType()).alias("product_id"),
            F.col("product_name"),
            F.col("aisle_name").alias("aisle"),
            F.col("department_name").alias("department"),
        )
    )

    product_dimension.overwrite(product_dim_df)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark)
