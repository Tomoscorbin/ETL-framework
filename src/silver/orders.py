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

orders = DeltaTable(
    table_name="orders",
    schema_name=Medallion.SILVER,
    catalog_name=settings.CATALOG,
    columns=[
        DeltaColumn(
            name="order_id",
            data_type=T.IntegerType(),
            is_primary_key=True,
            is_nullable=False,
        ),
        DeltaColumn(name="user_id", data_type=T.IntegerType(), is_nullable=False),
        DeltaColumn(name="evaluation_set", data_type=T.StringType(), is_nullable=False),
        DeltaColumn(name="order_number", data_type=T.IntegerType(), is_nullable=False),
        DeltaColumn(name="order_day_of_week", data_type=T.IntegerType(), is_nullable=False),
        DeltaColumn(name="order_hour", data_type=T.IntegerType(), is_nullable=False),
        DeltaColumn(name="days_since_prior", data_type=T.DoubleType()),
    ],
)


def main(spark: SparkSession) -> None:
    """Execute the pipeline."""
    raw_orders_df = spark.table(f"{settings.CATALOG}.{Medallion.BRONZE}.orders")
    orders_cleaned_df = raw_orders_df.select(
        F.col("order_id").cast(T.IntegerType()).alias("order_id"),
        F.col("user_id").cast(T.IntegerType()).alias("user_id"),
        F.col("eval_set").alias("evaluation_set"),
        F.col("order_number").cast(T.IntegerType()).alias("order_number"),
        F.col("order_dow").cast(T.IntegerType()).alias("order_day_of_week"),
        F.col("order_hour_of_day").cast(T.IntegerType()).alias("order_hour"),
        F.col("days_since_prior_order").cast(T.DoubleType()).alias("days_since_prior"),
    )

    orders.overwrite(orders_cleaned_df)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark)
