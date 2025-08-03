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

department = DeltaTable(
    table_name="department",
    schema_name=Medallion.SILVER,
    catalog_name=settings.CATALOG,
    comment="Reference data for departments",
    columns=[
        DeltaColumn(
            name="department_id",
            data_type=T.IntegerType(),
            is_primary_key=True,
            is_nullable=False,
            comment="Unique identifier for a department",
        ),
        DeltaColumn(
            name="department_name",
            data_type=T.StringType(),
            is_nullable=False,
            comment="Name of the department",
        ),
    ],
)


def clean_departments(df: DataFrame) -> DataFrame:
    """Alias and cast columns."""
    return df.select(
        F.col("department_id").cast(T.IntegerType()).alias("department_id"),
        F.col("department").alias("department_name"),
    )


def main(spark: SparkSession) -> None:
    """Execute the pipeline."""
    source_table_name = f"{settings.CATALOG}.{Medallion.BRONZE}.departments"
    raw_departments_df = spark.table(source_table_name)

    departments_cleaned_df = clean_departments(raw_departments_df)
    department.overwrite(departments_cleaned_df)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark)
