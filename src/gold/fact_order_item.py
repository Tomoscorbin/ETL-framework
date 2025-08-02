import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[1]))

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

from src import settings
from src.enums import Medallion
from src.models.column import DeltaColumn, ForeignKey
from src.models.table import DeltaTable

order_item_fact = DeltaTable(
    table_name="fact_order_item",
    schema_name=Medallion.GOLD,
    catalog_name=settings.CATALOG,
    columns=[
        DeltaColumn(
            name="order_id",
            data_type=T.IntegerType(),
            is_primary_key=True,
            is_nullable=False,
            comment="Identifier for the order",
            foreign_key=ForeignKey(table_name="order", column_name="order_id"),
        ),
        DeltaColumn(
            name="product_id",
            data_type=T.IntegerType(),
            is_primary_key=True,
            is_nullable=False,
            comment="Identifier for the product in the order",
            foreign_key=ForeignKey(table_name="dim_product", column_name="product_id"),
        ),
        DeltaColumn(
            name="user_id",
            data_type=T.IntegerType(),
            is_nullable=False,
            comment="User who placed the order",
        ),
        DeltaColumn(
            name="order_number",
            data_type=T.IntegerType(),
            is_nullable=False,
            comment="Sequential order number for the user",
        ),
        DeltaColumn(
            name="order_day_of_week",
            data_type=T.IntegerType(),
            is_nullable=False,
            comment="Day of week when the order was placed",
        ),
        DeltaColumn(
            name="order_hour",
            data_type=T.IntegerType(),
            is_nullable=False,
            comment="Hour of day when the order was placed",
        ),
        DeltaColumn(
            name="days_since_prior_order",
            data_type=T.IntegerType(),
            comment="Days since the user's previous order",
        ),
        DeltaColumn(
            name="add_to_cart_order",
            data_type=T.IntegerType(),
            is_nullable=False,
            comment="Sequence in which the product was added to the cart",
        ),
        DeltaColumn(
            name="reordered",
            data_type=T.IntegerType(),
            is_nullable=False,
            comment="1 if the product was reordered",
        ),
    ],
)


def main(spark: SparkSession) -> None:
    """Build the order item fact table."""
    prior_table = f"{settings.CATALOG}.{Medallion.BRONZE}.order_products__prior"
    train_table = f"{settings.CATALOG}.{Medallion.BRONZE}.order_products__train"
    orders_table = f"{settings.CATALOG}.{Medallion.SILVER}.order"

    prior_df = spark.table(prior_table)
    train_df = spark.table(train_table)
    order_products_df = prior_df.unionByName(train_df)

    orders_df = spark.table(orders_table)

    fact_df = order_products_df.join(orders_df, "order_id").select(
        F.col("order_id").cast(T.IntegerType()).alias("order_id"),
        F.col("product_id").cast(T.IntegerType()).alias("product_id"),
        F.col("user_id").cast(T.IntegerType()).alias("user_id"),
        F.col("order_number").cast(T.IntegerType()).alias("order_number"),
        F.col("order_day_of_week").cast(T.IntegerType()).alias("order_day_of_week"),
        F.col("order_hour").cast(T.IntegerType()).alias("order_hour"),
        F.col("days_since_prior_order").cast(T.IntegerType()).alias("days_since_prior_order"),
        F.col("add_to_cart_order").cast(T.IntegerType()).alias("add_to_cart_order"),
        F.col("reordered").cast(T.IntegerType()).alias("reordered"),
    )

    order_item_fact.overwrite(fact_df)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark)
