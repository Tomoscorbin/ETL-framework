import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[1]))

import pyspark.sql.types as T
from pyspark.sql import SparkSession

from src import settings
from src.bronze.config.utils import get_fully_qualified_name, load_table_config
from src.enums import Medallion
from src.models.table import DeltaColumn, DeltaTable

bronze_transaction_data = DeltaTable(
    catalog_name=settings.CATALOG,
    schema_name=Medallion.BRONZE,
    table_name="transaction_data",
    comment="Raw transaction data from source system",
    columns=[
        DeltaColumn("id", T.LongType(), is_nullable=True),
        DeltaColumn("date", T.TimestampType(), is_nullable=True),
        DeltaColumn("client_id", T.LongType(), is_nullable=True),
        DeltaColumn("card_id", T.LongType(), is_nullable=True),
        DeltaColumn("amount", T.StringType(), is_nullable=True),
        DeltaColumn("use_chip", T.StringType(), is_nullable=True),
        DeltaColumn("merchant_id", T.LongType(), is_nullable=True),
        DeltaColumn("merchant_city", T.StringType(), is_nullable=True),
        DeltaColumn("merchant_state", T.StringType(), is_nullable=True),
        DeltaColumn("zip", T.DoubleType(), is_nullable=True),
        DeltaColumn("mcc", T.LongType(), is_nullable=True),
        DeltaColumn("errors", T.StringType(), is_nullable=True),
    ],
)


def main(spark: SparkSession):
    """Execute the pipeline."""
    conf = load_table_config("transaction_source")
    source_table_name = get_fully_qualified_name(conf)
    source_df = spark.read.table(source_table_name)
    bronze_transaction_data.overwrite(source_df)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark)
