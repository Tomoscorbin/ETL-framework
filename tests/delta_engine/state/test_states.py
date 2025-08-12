import pytest
from dataclasses import FrozenInstanceError
from types import MappingProxyType
from collections.abc import Mapping
import pyspark.sql.types as T

from src.delta_engine.state.states import (
    ColumnState,
    PrimaryKeyState,
    TableState,
    CatalogState,
)


# ---------- ColumnState ----------

def test_column_state_is_frozen_and_holds_values():
    col = ColumnState(name="id", data_type=T.IntegerType(), is_nullable=False, comment="pk")
    assert col.name == "id"
    assert isinstance(col.data_type, T.IntegerType)
    assert col.is_nullable is False
    assert col.comment == "pk"
    with pytest.raises(FrozenInstanceError):
        col.name = "other"  # type: ignore[attr-defined]


# ---------- PrimaryKeyState ----------

def test_primary_key_state_tuple_and_frozen():
    pk = PrimaryKeyState(name="pk_tbl", columns=("id", "created_at"))
    assert pk.name == "pk_tbl"
    assert pk.columns == ("id", "created_at")
    with pytest.raises(FrozenInstanceError):
        pk.columns = ("id",)  # type: ignore[attr-defined]


# ---------- TableState ----------

def test_table_state_defaults_and_full_name():
    cols = (
        ColumnState("id", T.IntegerType(), False, "pk"),
        ColumnState("name", T.StringType(), True, ""),
    )
    ts = TableState(
        catalog_name="cat",
        schema_name="sch",
        table_name="tbl",
        exists=True,
        columns=cols,
        table_comment="hello",
    )

    assert ts.full_name == "cat.sch.tbl"
    assert ts.exists is True
    assert ts.columns == cols  # tuple preserved
    assert isinstance(ts.table_properties, Mapping)
    assert ts.table_properties == {}  # default is empty mapping
    assert ts.primary_key is None

    # frozen dataclass behavior
    with pytest.raises(FrozenInstanceError):
        ts.exists = False  # type: ignore[attr-defined]


def test_table_state_has_slots_and_no_dynamic_attrs():
    assert hasattr(TableState, "__slots__")
    assert "catalog_name" in TableState.__slots__

    ts = TableState.empty("c", "s", "t")

    # assigning a new attribute should fail (slots + frozen)
    with pytest.raises((AttributeError, FrozenInstanceError, TypeError)):
        setattr(ts, "new_attr", 1)



def test_table_state_default_table_properties_is_read_only_mappingproxy():
    ts = TableState(
        catalog_name="c",
        schema_name="s",
        table_name="t",
        exists=True,
    )
    assert isinstance(ts.table_properties, MappingProxyType)
    with pytest.raises(TypeError):
        ts.table_properties["x"] = "y"  # type: ignore[index]


def test_table_state_empty_factory_produces_nonexistent_defaults():
    ts = TableState.empty("cat", "sch", "tbl")
    assert ts.full_name == "cat.sch.tbl"
    assert ts.exists is False
    assert ts.columns == ()
    assert ts.table_comment == ""
    assert isinstance(ts.table_properties, Mapping)
    assert ts.table_properties == {}
    assert ts.primary_key is None


# ---------- CatalogState ----------

def test_catalog_state_get_returns_expected_tablestate_or_none():
    present = TableState(
        catalog_name="c",
        schema_name="s",
        table_name="t",
        exists=True,
        columns=(),
    )
    missing = TableState.empty("c", "s", "u")  # not inserted

    snap = CatalogState(tables={present.full_name: present})
    assert snap.get("c", "s", "t") is present
    assert snap.get("c", "s", "u") is None
