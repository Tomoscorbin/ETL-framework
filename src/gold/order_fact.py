import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[1]))

import pyspark.sql.types as T
from pyspark.sql import SparkSession

from src import settings
from src.enums import Medallion
from src.models.column import DeltaColumn
from src.models.table import DeltaTable
from src.silver.order import order

order_fact = DeltaTable(
    table_name="order_fact",
    schema_name=Medallion.GOLD,
    catalog_name=settings.CATALOG,
    comment="Fact table capturing core metrics for each order",
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


def main(spark: SparkSession) -> None:
    """Build the order fact table."""
    order_df = order.read(spark)
    order_df_selected = order_df.select(
        "order_id",
        "user_id",
        "order_number",
        "order_day_of_week",
        "order_hour",
        "days_since_prior_order",
    )

    order_fact.overwrite(order_df_selected)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark)
