import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[1]))

from src import settings
from table import DeltaColumn, DeltaTable
from table_manager import DeltaTableManager
import pyspark.sql.types as T


transactions = DeltaTable(
    table_name="transactions",
    schema_name="silver",
    catalog_name=settings.CATALOG,
    columns = [
        DeltaColumn(
            name="id",
            data_type=T.IntegerType(),
            is_primary_key=True,
            is_nullable=False
        ),
        DeltaColumn(
            name="date",
            data_type=T.DateType(),
            is_nullable=False
        ),
        DeltaColumn(
            name="client_id",
            data_type=T.IntegerType(),
            is_nullable=False
        ),
        DeltaColumn(
            name="card_id",
            data_type=T.IntegerType(),
            is_nullable=False
        ),
        DeltaColumn(
            name="amount",
            data_type=T.DecimalType(6,2),
            is_nullable=False
        ),
        DeltaColumn(
            name="transaction_type",
            data_type=T.StringType(),
            is_nullable=False
        ),
        DeltaColumn(
            name="mcc",
            data_type=T.StringType(),
            is_nullable=False
        ),
    ]
)
