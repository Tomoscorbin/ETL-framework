import pytest
from pyspark.sql import types as T

from src.models.table import DeltaColumn, DeltaTable


@pytest.fixture(scope="module")
def example_columns():
    return [
        DeltaColumn("id", T.IntegerType(), is_primary_key=True, is_nullable=False),
        DeltaColumn("foo", T.StringType(), is_nullable=True),
    ]


@pytest.fixture
def delta_table(example_columns):
    return DeltaTable(
        table_name="my_table",
        schema_name="public",
        catalog_name="main",
        comment="Table used for unit tests",
        columns=example_columns,
    )


def test_full_name(delta_table):
    assert delta_table.full_name == "main.public.my_table"


def test_primary_keys(delta_table):
    assert delta_table.primary_key_column_names == ["id"]


def test_schema_roundtrip(delta_table):
    struct = delta_table.schema
    names = [f.name for f in struct.fields]
    assert names == ["id", "foo"]
