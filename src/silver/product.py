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

product = DeltaTable(
    table_name="product",
    schema_name=Medallion.SILVER,
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
            foreign_key=ForeignKey(table_name="aisles", column_name="aisle_id"),
        ),
        DeltaColumn(
            name="department_id",
            data_type=T.IntegerType(),
            is_nullable=False,
            comment="Identifier of the department for the product",
            foreign_key=ForeignKey(table_name="departments", column_name="department_id"),
        ),
    ],
)


NUMERIC_ONLY_REGEX: str = r"^\d+$"


def main(spark: SparkSession) -> None:
    """Execute the pipeline."""
    source_table_name = f"{settings.CATALOG}.{Medallion.BRONZE}.products"
    raw_products_df = spark.table(source_table_name)

    is_numeric_aisle_id = F.col("aisle_id").rlike(NUMERIC_ONLY_REGEX)
    is_numeric_department_id = F.col("department_id").rlike(NUMERIC_ONLY_REGEX)

    products_cleaned_df = (
        raw_products_df
        .filter(is_numeric_aisle_id)
        .filter(is_numeric_department_id)
        .select(
            "product_name",
            F.col("product_id").cast(T.IntegerType()).alias("product_id"),
            F.col("aisle_id").cast(T.IntegerType()).alias("aisle_id"),
            F.col("department_id").cast(T.IntegerType()).alias("department_id"),
        )
    )

    product.overwrite(products_cleaned_df)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark)
