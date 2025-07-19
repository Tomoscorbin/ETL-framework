import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[1]))

import pyspark.sql.types as T

from src import settings
from src.models.table import DeltaColumn, DeltaTable

data_quality_checks = DeltaTable(
    table_name="data_quality_checks",
    schema_name="metadata",
    catalog_name=settings.CATALOG,
    columns=[
        DeltaColumn(name="check_name", data_type=T.StringType(), is_nullable=False),
        DeltaColumn(name="columns", data_type=T.StringType(), is_nullable=False),
        DeltaColumn(name="function", data_type=T.StringType(), is_nullable=False),
        DeltaColumn(name="run_time", data_type=T.TimestampType(), is_nullable=False),
    ],
)
