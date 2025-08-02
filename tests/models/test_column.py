from pyspark.sql import types as T

from src.models.column import DeltaColumn, ForeignKey


class TestStructField:
    def test_struct_field_non_nullable(self):
        input_column = DeltaColumn(name="foo", data_type=T.StringType(), is_nullable=False)
        expected = T.StructField(name="foo", dataType=T.StringType(), nullable=False)
        actual = input_column.struct_field
        assert expected == actual

    def test_foreign_key(self):
        fk = ForeignKey(table_name="bar", column_name="id")
        column = DeltaColumn(name="foo_id", data_type=T.IntegerType(), foreign_key=fk)
        assert column.foreign_key == fk

    def test_struct_field_nullable(self):
        input_column = DeltaColumn(name="foo", data_type=T.StringType(), is_nullable=True)
        expected = T.StructField(name="foo", dataType=T.StringType(), nullable=True)
        actual = input_column.struct_field
        assert expected == actual
