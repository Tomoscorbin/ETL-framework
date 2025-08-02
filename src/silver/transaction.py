import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[1]))

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, SparkSession

from databricks.labs.dqx import check_funcs  # type: ignore
from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule  # type: ignore
from src import settings
from src.enums import Medallion
from src.models.column import DeltaColumn
from src.models.table import DeltaTable

transaction = DeltaTable(
    table_name="transaction",
    schema_name=Medallion.SILVER,
    catalog_name=settings.CATALOG,
    columns=[
        DeltaColumn(name="id", data_type=T.LongType(), is_primary_key=True, is_nullable=False),
        DeltaColumn(name="timestamp", data_type=T.TimestampType(), is_nullable=False),
        DeltaColumn(name="client_id", data_type=T.LongType(), is_nullable=False),
        DeltaColumn(name="card_id", data_type=T.LongType(), is_nullable=False),
        DeltaColumn(name="amount", data_type=T.DecimalType(6, 2), is_nullable=False),
        DeltaColumn(name="transaction_type", data_type=T.StringType(), is_nullable=False),
        DeltaColumn(name="mcc", data_type=T.LongType(), is_nullable=False),
    ],
    rules=[
        DQDatasetRule(criticality="error", check_func=check_funcs.is_unique, columns=["id"]),
        DQRowRule(
            criticality="warn",
            check_func=check_funcs.is_in_list,
            column="transaction_type",
            check_func_args=[["Correct value"]],
        ),
    ],
)


def currency_to_decimal(
    column_name: str,
    precision: int = 6,
    scale: int = 2,
) -> Column:
    """Converts string amounts to decimal by remove any currency symbols, commas, or spaces."""
    column_trimmed = F.trim(column_name)
    symbols_to_remove = r"[\$\£\€\,\s]"
    symbols_stripped = F.regexp_replace(column_trimmed, symbols_to_remove, "")
    return symbols_stripped.cast(T.DecimalType(precision, scale))


def main(spark: SparkSession) -> None:
    """Execute the pipeline."""
    raw_transactions_df = spark.table("source.raw.transactions_data")
    transactions_cleaned_df = raw_transactions_df.select(
        "id",
        "client_id",
        "card_id",
        "mcc",
        currency_to_decimal("amount").alias("amount"),
        F.col("date").alias("timestamp"),
        F.col("use_chip").alias("transaction_type"),
    )

    transaction.overwrite(transactions_cleaned_df)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark)
