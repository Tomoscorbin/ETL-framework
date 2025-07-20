import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[1]))

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column

from databricks.labs.dqx import check_funcs  # type: ignore
from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule  # type: ignore
from src import settings
from src.models.table import DeltaColumn, DeltaTable

transaction = DeltaTable(
    table_name="transaction",
    schema_name="silver",
    catalog_name=settings.CATALOG,
    columns=[
        DeltaColumn(name="id", data_type=T.LongType(), is_primary_key=True, is_nullable=False),
        DeltaColumn(name="timestamp", data_type=T.DateType(), is_nullable=False),
        DeltaColumn(name="client_id", data_type=T.LongType(), is_nullable=False),
        DeltaColumn(name="card_id", data_type=T.LongType(), is_nullable=False),
        DeltaColumn(name="amount", data_type=T.DecimalType(6, 2), is_nullable=False),
        DeltaColumn(name="transaction_type", data_type=T.StringType(), is_nullable=False),
        DeltaColumn(name="mcc", data_type=T.StringType(), is_nullable=False),
    ],
    rules=[
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_in_list,
            column="transaction_type",
            check_func_args=[["Correct value"]],
        ),
        DQDatasetRule(criticality="error", check_func=check_funcs.is_unique, columns=["id"]),
    ],
)


def currency_to_decimal(
    column_name: str,
    precision: int = 6,
    scale: int = 2,
) -> Column:
    """Converts string amounts to decimal."""
    # Remove any currency symbols, commas, and spaces
    stripped = F.regexp_replace(F.trim(column_name), r"[\$\£\€\,\s]", "")
    return stripped.cast(T.DecimalType(precision, scale))


source_df = settings.spark.table("source.raw.transactions_data")
transactions_cleaned_df = source_df.select(
    "id",
    "client_id",
    "card_id",
    "mcc",
    F.col("date").alias("timestamp"),
    currency_to_decimal("amount").alias("amount"),
    F.col("use_chip").alias("transaction_type"),
)

if __name__ == "__main__":
    transaction.overwrite(transactions_cleaned_df)
