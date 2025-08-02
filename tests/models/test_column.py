from pyspark.sql import types as T

from src.models.column import DeltaColumn


class TestStructField:
    def test_struct_field_non_nullable(self):
        input_column = DeltaColumn(name="foo", data_type=T.StringType(), is_nullable=False)
        expected = T.StructField(name="foo", dataType=T.StringType(), nullable=False)
        actual = input_column.struct_field
        assert expected == actual

    def test_struct_field_nullable(self):
        input_column = DeltaColumn(name="foo", data_type=T.StringType(), is_nullable=True)
        expected = T.StructField(name="foo", dataType=T.StringType(), nullable=True)
        actual = input_column.struct_field
        assert expected == actual
