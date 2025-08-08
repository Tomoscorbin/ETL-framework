"""Transform source order data into the silver layer."""

import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[1]))

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from src import settings
from src.enums import Medallion
from src.models.column import DeltaColumn, ForeignKey
from src.models.table import DeltaTable
from src.silver.product import product

order = DeltaTable(
    table_name="order",
    schema_name=Medallion.SILVER,
    catalog_name=settings.CATALOG,
    comment="Cleaned order data capturing user and timing details",
    columns=[
        DeltaColumn(
            name="order_id",
            data_type=T.IntegerType(),
            is_primary_key=True,
            is_nullable=False,
            comment="Unique identifier for the order",
        ),
        DeltaColumn(
            name="user_id",
            data_type=T.IntegerType(),
            is_nullable=False,
            comment="Identifier for the user who placed the order",
        ),
        DeltaColumn(
            name="product_id",
            data_type=T.IntegerType(),
            is_nullable=True,
            comment="Unique identifier for a product",
            foreign_key=ForeignKey(
                target_table=product, target_column="product_id"
            ),
        ),
        DeltaColumn(
            name="order_number",
            data_type=T.IntegerType(),
            is_nullable=False,
            comment="Sequential number of the order for the user",
        ),
        DeltaColumn(
            name="order_day_of_week",
            data_type=T.IntegerType(),
            is_nullable=False,
            comment="Day of the week when the order was placed",
        ),
        DeltaColumn(
            name="order_hour",
            data_type=T.IntegerType(),
            is_nullable=False,
            comment="Hour of the day the order was placed",
        ),
        DeltaColumn(
            name="days_since_prior_order",
            data_type=T.IntegerType(),
            comment="Days elapsed since the previous order",
        ),
    ],
)


def clean_orders(df: DataFrame) -> DataFrame:
    """Alias and cast columns."""
    return df.select(
        F.col("order_id").cast(T.IntegerType()).alias("order_id"),
        F.col("user_id").cast(T.IntegerType()).alias("user_id"),
        F.col("product_id").cast(T.IntegerType()).alias("product_id"),
        F.col("order_number").cast(T.IntegerType()).alias("order_number"),
        F.col("order_dow").cast(T.IntegerType()).alias("order_day_of_week"),
        F.col("order_hour_of_day").cast(T.IntegerType()).alias("order_hour"),
        F.col("days_since_prior_order").cast(T.IntegerType()).alias("days_since_prior_order"),
    )


def main(spark: SparkSession) -> None:
    """Execute the pipeline."""
    source_table_name = f"{settings.CATALOG}.{Medallion.BRONZE}.orders"
    raw_orders_df = spark.table(source_table_name)

    orders_cleaned_df = clean_orders(raw_orders_df)
    order.overwrite(orders_cleaned_df)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark)
