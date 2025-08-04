import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[2]))

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from src import settings
from src.enums import DQCriticality, Medallion
from src.metadata.data_quality.data_quality_checks import data_quality_checks
from src.models.column import DeltaColumn
from src.models.table import DeltaTable

quality = DeltaTable(
    table_name="quality",
    schema_name=Medallion.METADATA,
    catalog_name=settings.CATALOG,
    columns=[
        DeltaColumn(
            name="date",
            data_type=T.DateType(),
            is_nullable=False,
            is_primary_key=True,
        ),
        DeltaColumn(
            name="error_count",
            data_type=T.IntegerType(),
            is_nullable=False,
        ),
        DeltaColumn(
            name="warning_count",
            data_type=T.IntegerType(),
            is_nullable=False,
        ),
    ],
)


def derive_quality_metrics(data_quality_df: DataFrame) -> DataFrame:
    """Derive quality metrics from the data quality check log."""
    is_today = F.col("date") == F.current_date()
    is_error = F.col("criticality") == DQCriticality.ERROR
    is_warning = F.col("criticality") == DQCriticality.WARN
    
    error_indicator = F.when(is_error, 1).otherwise(0)
    warning_indicator = F.when(is_warning, 1).otherwise(0)
    error_count = F.sum(error_indicator)
    warning_count = F.sum(warning_indicator)

    todays_failures = data_quality_df.filter(is_today)
    return todays_failures.agg(
        F.current_date().alias("date"),
        error_count.alias("error_count"),
        warning_count.alias("warning_count"),
    )


def main(spark: SparkSession) -> None:
    """Execute."""
    dq_df = data_quality_checks.read(spark)
    final_df = derive_quality_metrics(dq_df)
    quality.merge(final_df)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark)