import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[1]))

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from src import settings
from src.enums import Medallion
from src.models.column import DeltaColumn
from src.models.table import DeltaTable
from src.silver.aisle import aisle
from src.silver.department import department
from src.silver.product import product

product_dimension = DeltaTable(
    table_name="product_dimension",
    schema_name=Medallion.GOLD,
    catalog_name=settings.CATALOG,
    comment="Dimension table combining product, aisle, and department details",
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
            name="aisle_name",
            data_type=T.StringType(),
            is_nullable=False,
            comment="Name of the aisle containing the product",
        ),
        DeltaColumn(
            name="department_name",
            data_type=T.StringType(),
            is_nullable=False,
            comment="Name of the department for the product",
        ),
    ],
)


def combine_dataframes(
    product_df: DataFrame, aisle_df: DataFrame, department_df: DataFrame
) -> DataFrame:
    """Join product, aisle, and department data into a single DataFrame."""
    return product_df.join(
        aisle_df,
        on="aisle_id",
        how="inner",
    ).join(
        department_df,
        on="department_id",
        how="inner",
    )


def main(spark: SparkSession) -> None:
    """Build the product dimension table."""
    aisle_df = aisle.read(spark)
    department_df = department.read(spark)
    product_df = product.read(spark)

    joined_df = combine_dataframes(product_df, aisle_df, department_df)

    product_dim_df = joined_df.select(
        "product_id",
        "product_name",
        F.col("aisle_name").alias("aisle_name"),
        F.col("department_name").alias("department_name"),
    )

    product_dimension.overwrite(product_dim_df)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark)
