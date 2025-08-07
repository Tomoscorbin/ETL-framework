from dataclasses import FrozenInstanceError

import pytest
from pyspark.sql import types as T

from src.models.column import DeltaColumn, ForeignKey


@pytest.mark.parametrize(
    ("name", "dtype", "nullable"),
    [
        ("foo", T.StringType(), False),  # non-nullable string
        ("bar", T.IntegerType(), True),  # nullable int
        ("ts", T.TimestampType(), True),  # nullable string
    ],
)
def test_struct_field_values(name, dtype, nullable):
    col = DeltaColumn(name=name, data_type=dtype, is_nullable=nullable)

    sf = col.struct_field

    assert isinstance(sf, T.StructField)
    assert isinstance(sf.dataType, type(dtype))
    assert sf.name == name
    assert sf.nullable is nullable


def test_foreign_key():
    fk = ForeignKey(reference_table_full_name="bar", column_name="id")
    column = DeltaColumn(name="foo_id", data_type=T.IntegerType(), foreign_key=fk)
    assert column.foreign_key == fk


def test_delta_column_is_frozen():
    col = DeltaColumn("foo", T.StringType())
    with pytest.raises(FrozenInstanceError):
        col.name = "oops"
