import pytest
from dataclasses import FrozenInstanceError
from collections.abc import Mapping
import pyspark.sql.types as T

from src.delta_engine.models import Column, Table
from src.enums import DeltaTableProperty


def make_demo_table(**overrides) -> Table:
    columns = [
        Column(name="id", data_type=T.IntegerType(), is_nullable=False),
        Column(name="name", data_type=T.StringType(), comment="customer name"),
        Column(name="created_at", data_type=T.TimestampType()),
    ]
    base = dict(
        catalog_name="retail",
        schema_name="silver",
        table_name="customers",
        columns=columns,
        comment="Customer dimension",
        table_properties={},
        primary_key=["id"],
    )
    base.update(overrides)
    return Table(**base)


def test_full_name_is_fully_qualified():
    table = make_demo_table()
    assert table.full_name == "retail.silver.customers"


def test_column_names_preserve_order():
    table = make_demo_table()
    assert table.column_names == ["id", "name", "created_at"]


def test_effective_table_properties_include_defaults_and_merge_overrides():
    # override COLUMN_MAPPING_MODE and add an extra property
    user_props = {
        DeltaTableProperty.COLUMN_MAPPING_MODE: "id",
        "delta.appendOnly": "true",
    }
    table = make_demo_table(table_properties=user_props)

    props = table.effective_table_properties
    # It should look/act like a read-only Mapping
    assert isinstance(props, Mapping)

    # Default present but overridden
    assert props[DeltaTableProperty.COLUMN_MAPPING_MODE] == "id"
    # Extra user property retained
    assert props["delta.appendOnly"] == "true"


def test_effective_table_properties_is_read_only():
    table = make_demo_table()
    props = table.effective_table_properties
    with pytest.raises(TypeError):
        props["x"] = "y"


def test_column_is_frozen_dataclass():
    col = Column(name="id", data_type=T.IntegerType(), is_nullable=False)
    with pytest.raises(FrozenInstanceError):
        col.name = "new_id"  # type: ignore[attr-defined]


def test_table_is_frozen_dataclass():
    table = make_demo_table()
    with pytest.raises(FrozenInstanceError):
        table.comment = "different"  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "primary_key",
    (None, ["id"], ["id", "created_at"]),
)
def test_primary_key_optional(primary_key):
    table = make_demo_table(primary_key=primary_key)
    assert table.primary_key == primary_key


def test_column_names_empty_when_no_columns():
    empty_table = make_demo_table(columns=[])
    assert empty_table.column_names == []
