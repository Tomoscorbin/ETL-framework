import pyspark.sql.types as T

from src.bronze.ingest import columns_from_schema
from src.models.column import DeltaColumn


def test_columns_from_schema():
    input_schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), True),
            T.StructField("name", T.StringType(), False),
        ]
    )
    expected = [
        DeltaColumn("id", T.IntegerType(), is_nullable=True),
        DeltaColumn("name", T.StringType(), is_nullable=False),
    ]

    actual = columns_from_schema(input_schema)

    assert expected == actual
