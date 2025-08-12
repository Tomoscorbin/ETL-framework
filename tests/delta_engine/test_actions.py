# tests/test_actions.py
import pytest
from dataclasses import FrozenInstanceError
from types import MappingProxyType
from collections.abc import Mapping
import pyspark.sql.types as T

from src.delta_engine.actions import (
    PrimaryKeyDefinition,
    ColumnAdd,
    ColumnDrop,
    ColumnNullabilityChange,
    SetColumnComments,
    SetTableComment,
    SetTableProperties,
    PrimaryKeyAdd,
    PrimaryKeyDrop,
    CreateTable,
    AlignTable,
    TablePlan,
)


# ---- Helpers ----

def make_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("id", T.IntegerType(), nullable=False),
            T.StructField("name", T.StringType(), nullable=True),
        ]
    )

def make_pk() -> PrimaryKeyDefinition:
    return PrimaryKeyDefinition(name="pk_customers", columns=("id",))

def make_create_table(with_pk: bool = False) -> CreateTable:
    return CreateTable(
        catalog_name="retail",
        schema_name="silver",
        table_name="customers",
        schema_struct=make_schema(),
        table_comment="customer master",
        table_properties={"delta.appendOnly": "false"},
        column_comments={"id": "surrogate id"},
        primary_key=make_pk() if with_pk else None,
    )


# ---- Core payloads ----

def test_primary_key_definition_is_tuple_and_frozen():
    pk = make_pk()
    assert pk.name == "pk_customers"
    assert pk.columns == ("id",)
    with pytest.raises(FrozenInstanceError):
        pk.name = "other"  # type: ignore[attr-defined]
    with pytest.raises(FrozenInstanceError):
        pk.columns = ("id", "name")  # type: ignore[attr-defined]


def test_column_add_defaults_and_values():
    add = ColumnAdd(name="age", data_type=T.IntegerType(), is_nullable=True)
    assert add.name == "age"
    assert isinstance(add.data_type, T.IntegerType)
    assert add.is_nullable is True
    assert add.comment == ""  # default


def test_column_drop_simple():
    drop = ColumnDrop(name="legacy_flag")
    assert drop.name == "legacy_flag"


def test_nullability_change_both_directions():
    relax = ColumnNullabilityChange(name="name", make_nullable=True)
    tighten = ColumnNullabilityChange(name="id", make_nullable=False)
    assert relax.make_nullable is True
    assert tighten.make_nullable is False


def test_set_column_comments_accepts_mapping_and_is_read_only_if_mappingproxy_used():
    mp = MappingProxyType({"id": "surrogate key", "name": "customer name"})
    op = SetColumnComments(comments=mp)
    assert isinstance(op.comments, Mapping)
    assert op.comments["id"] == "surrogate key"
    with pytest.raises(TypeError):
        op.comments["id"] = "nope"  # mappingproxy is read-only


def test_set_table_comment_and_properties():
    comment = SetTableComment(comment="fresh table")
    props = SetTableProperties(properties={"delta.autoOptimize.optimizeWrite": "true"})
    assert comment.comment == "fresh table"
    assert props.properties["delta.autoOptimize.optimizeWrite"] == "true"


def test_primary_key_add_and_drop_wrap_payloads():
    add = PrimaryKeyAdd(definition=make_pk())
    drop = PrimaryKeyDrop(name="pk_customers")
    assert add.definition.columns == ("id",)
    assert drop.name == "pk_customers"


# ---- CreateTable ----

def test_create_table_without_pk():
    ct = make_create_table(with_pk=False)
    assert ct.catalog_name == "retail"
    assert ct.schema_name == "silver"
    assert ct.table_name == "customers"
    assert isinstance(ct.schema_struct, T.StructType)
    assert ct.table_comment == "customer master"
    assert ct.table_properties["delta.appendOnly"] == "false"
    assert ct.column_comments["id"] == "surrogate id"
    assert ct.primary_key is None


def test_create_table_with_pk():
    ct = make_create_table(with_pk=True)
    assert isinstance(ct.primary_key, PrimaryKeyDefinition)
    assert ct.primary_key.columns == ("id",)


def test_create_table_is_frozen():
    ct = make_create_table()
    with pytest.raises(FrozenInstanceError):
        ct.table_name = "other"  # type: ignore[attr-defined]


# ---- AlignTable ----

def test_align_table_defaults_are_empty_and_none():
    at = AlignTable(catalog_name="retail", schema_name="silver", table_name="customers")
    assert isinstance(at.add_columns, tuple) and at.add_columns == ()
    assert isinstance(at.drop_columns, tuple) and at.drop_columns == ()
    assert isinstance(at.change_nullability, tuple) and at.change_nullability == ()
    assert at.set_column_comments is None
    assert at.set_table_comment is None
    assert at.set_table_properties is None
    assert at.drop_primary_key is None
    assert at.add_primary_key is None


def test_align_table_with_mixed_operations_and_pk_edits():
    at = AlignTable(
        catalog_name="retail",
        schema_name="silver",
        table_name="customers",
        add_columns=(ColumnAdd("age", T.IntegerType(), True),),
        drop_columns=(ColumnDrop("legacy_flag"),),
        change_nullability=(ColumnNullabilityChange("name", make_nullable=False),),
        set_column_comments=SetColumnComments({"age": "years"}),
        set_table_comment=SetTableComment("updated comment"),
        set_table_properties=SetTableProperties({"delta.appendOnly": "true"}),
        drop_primary_key=PrimaryKeyDrop("pk_customers_old"),
        add_primary_key=PrimaryKeyAdd(PrimaryKeyDefinition("pk_customers_new", ("id",))),
    )
    assert at.add_columns[0].name == "age"
    assert at.drop_columns[0].name == "legacy_flag"
    assert at.change_nullability[0].name == "name"
    assert at.set_column_comments.comments["age"] == "years"
    assert at.set_table_comment.comment == "updated comment"
    assert at.set_table_properties.properties["delta.appendOnly"] == "true"
    assert at.drop_primary_key.name == "pk_customers_old"
    assert at.add_primary_key.definition.name == "pk_customers_new"


def test_align_table_is_frozen():
    at = AlignTable(catalog_name="retail", schema_name="silver", table_name="customers")
    with pytest.raises(FrozenInstanceError):
        at.table_name = "other"  # type: ignore[attr-defined]


# ---- TablePlan ----

def test_table_plan_holds_create_and_align_and_is_frozen():
    ct = make_create_table()
    at = AlignTable(catalog_name="retail", schema_name="silver", table_name="customers")
    plan = TablePlan(create_tables=(ct,), align_tables=(at,))
    assert plan.create_tables[0] is ct
    assert plan.align_tables[0] is at
    with pytest.raises(FrozenInstanceError):
        plan.create_tables = tuple()  # type: ignore[attr-defined]
