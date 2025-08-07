"""Builds the order fact table in the gold layer."""

import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[1]))

import pyspark.sql.types as T
from pyspark.sql import SparkSession

from src import settings
from src.enums import DQCriticality, Medallion
from src.gold.product_dimension import product_dimension
from src.models.column import DeltaColumn, ForeignKey, QualityRule
from src.models.data_quality_table import DQDeltaTable
from src.silver.order import order

order_fact = DQDeltaTable(
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
            name="product_id",
            data_type=T.IntegerType(),
            is_nullable=True,
            comment="Unique identifier for a product",
            foreign_key=ForeignKey(
                reference_table_full_name=product_dimension.full_name,
                reference_column_name="product_id",
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
            quality_rule=QualityRule(min_value=0, criticality=DQCriticality.WARN),
        ),
    ],
)


def main(spark: SparkSession) -> None:
    """Build the order fact table."""
    order_df = order.read(spark)
    order_df_selected = order_df.select(
        "order_id",
        "user_id",
        "product_id",
        "order_number",
        "order_day_of_week",
        "order_hour",
        "days_since_prior_order",
    )

    order_fact.overwrite(order_df_selected)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark)
