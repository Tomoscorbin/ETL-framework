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

product_dim = DeltaTable(
    table_name="product_dimension",
    schema_name=Medallion.GOLD,
    catalog_name=settings.CATALOG,
    columns=[
        DeltaColumn(
            name="product_id",
            data_type=T.IntegerType(),
            is_primary_key=True,
            is_nullable=False,
            comment="Unique identifier for a product",
        ),
        DeltaColumn(
            name="product_name",
            data_type=T.StringType(),
            is_nullable=False,
            comment="Name of the product",
        ),
        DeltaColumn(
            name="aisle_id",
            data_type=T.IntegerType(),
            is_nullable=False,
            comment="Identifier of the aisle containing the product",
        ),
        DeltaColumn(
            name="aisle_name",
            data_type=T.StringType(),
            is_nullable=False,
            comment="Name of the aisle containing the product",
        ),
        DeltaColumn(
            name="department_id",
            data_type=T.IntegerType(),
            is_nullable=False,
            comment="Identifier of the department for the product",
        ),
        DeltaColumn(
            name="department_name",
            data_type=T.StringType(),
            is_nullable=False,
            comment="Name of the department for the product",
        ),
    ],
)


def main(spark: SparkSession) -> None:
    """Build the product dimension table."""
    product_df = spark.table(f"{settings.CATALOG}.{Medallion.SILVER}.product")
    aisle_df = spark.table(f"{settings.CATALOG}.{Medallion.SILVER}.aisle")
    department_df = spark.table(f"{settings.CATALOG}.{Medallion.SILVER}.department")

    product_dim_df = (
        product_df.join(aisle_df, on="aisle_id")
        .join(department_df, on="department_id")
        .select(
            "product_id",
            "product_name",
            "aisle_id",
            F.col("aisle_name").alias("aisle_name"),
            "department_id",
            F.col("department_name").alias("department_name"),
        )
    )

    product_dim.overwrite(product_dim_df)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark)
