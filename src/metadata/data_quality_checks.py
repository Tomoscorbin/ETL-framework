import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[1]))

import pyspark.sql.types as T

from src import settings
from src.constants import DATA_QUALITY_TABLE_NAME
from src.enums import Medallion
from src.models.table import DeltaColumn, DeltaTable

data_quality_checks: DeltaTable = DeltaTable(
    table_name=DATA_QUALITY_TABLE_NAME,
    schema_name=Medallion.METADATA,
    catalog_name=settings.CATALOG,
    columns=[
        DeltaColumn(name="date", data_type=T.DateType(), is_nullable=False),
        DeltaColumn(name="severity", data_type=T.StringType(), is_nullable=False),
        DeltaColumn(name="table_name", data_type=T.StringType(), is_nullable=False),
        DeltaColumn(name="check_name", data_type=T.StringType(), is_nullable=False),
        DeltaColumn(name="columns", data_type=T.ArrayType(T.StringType()), is_nullable=False),
        DeltaColumn(name="function", data_type=T.StringType(), is_nullable=False),
        DeltaColumn(name="run_time", data_type=T.TimestampType(), is_nullable=False),
        DeltaColumn(name="job_id", data_type=T.LongType(), is_nullable=False),
        DeltaColumn(name="run_id", data_type=T.LongType(), is_nullable=False),
    ],
)
