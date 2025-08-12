from types import MappingProxyType

import pyspark.sql.types as T

from src.delta_engine.actions import (
    AlignTable,
    ColumnAdd,
    ColumnDrop,
    ColumnNullabilityChange,
    CreateTable,
    PrimaryKeyAdd,
    PrimaryKeyDrop,
    SetColumnComments,
    SetTableComment,
    SetTableProperties,
)
from src.delta_engine.compile.planner import Planner
from src.delta_engine.models import Column, Table
from src.delta_engine.state.states import (
    CatalogState,
    ColumnState,
    PrimaryKeyState,
    TableState,
)
from src.delta_engine.actions import PrimaryKeyDefinition


# ----------------- helpers -----------------


def str_keys(m):
    """Return a dict with keys coerced to strings (enum.value if enum)."""
    return {(k.value if hasattr(k, "value") else k): v for k, v in m.items()}


def make_struct_fields(names_types_nulls):
    return [T.StructField(n, dt, nullable=nl) for n, dt, nl in names_types_nulls]


def table_state(
    *,
    catalog="cat",
    schema="sch",
    table="tbl",
    exists=True,
    columns=(),
    table_comment="",
    table_properties=None,
    pk: PrimaryKeyState | None = None,
) -> TableState:
    if table_properties is None:
        table_properties = MappingProxyType({})
    return TableState(
        catalog_name=catalog,
        schema_name=schema,
        table_name=table,
        exists=exists,
        columns=tuple(columns),
        table_comment=table_comment,
        table_properties=table_properties,
        primary_key=pk,
    )


def desired_table(
    *,
    catalog="cat",
    schema="sch",
    table="tbl",
    cols: list[Column],
    comment: str = "",
    table_props: dict[str, str] | None = None,
    pk_cols: list[str] | None = None,
) -> Table:
    return Table(
        catalog_name=catalog,
        schema_name=schema,
        table_name=table,
        columns=cols,
        comment=comment,
        table_properties=table_props or {},
        primary_key=pk_cols,
    )


# ----------------- create path -----------------


def test_plan_emits_create_when_table_missing():
    planner = Planner()
    desired = desired_table(
        cols=[
            Column("id", T.IntegerType(), is_nullable=False, comment="pk"),
            Column("name", T.StringType(), is_nullable=True, comment=""),
        ],
        comment="customers",
        table_props={"delta.appendOnly": "true"},
        pk_cols=["id"],
    )
    state = CatalogState(tables={})  # table not present

    plan = planner.plan([desired], state)
    ct: CreateTable = plan.create_tables[0]
    fields = ct.schema_struct.fields

    assert isinstance(plan.create_tables, tuple)
    assert isinstance(plan.align_tables, tuple)
    assert len(plan.create_tables) == 1
    assert len(plan.align_tables) == 0

    assert ct.catalog_name == "cat"
    assert ct.table_name == "tbl"

    # schema
    assert [(f.name, type(f.dataType), f.nullable) for f in fields] == [
        ("id", T.IntegerType, False),
        ("name", T.StringType, True),
    ]
    # comments mapped
    assert ct.column_comments == {"id": "pk", "name": ""}
    # properties include desired + defaults (at minimum should carry desired)
    assert ct.table_properties["delta.appendOnly"] == "true"
    # PK definition present and columns correct
    assert ct.primary_key is not None
    assert ct.primary_key.columns == ("id",)


def test_plan_emits_create_when_state_entry_exists_false():
    planner = Planner()
    desired = desired_table(cols=[Column("id", T.IntegerType(), is_nullable=False)])
    snap = CatalogState(tables={"cat.sch.tbl": table_state(exists=False)})
    plan = planner.plan([desired], snap)
    assert len(plan.create_tables) == 1
    assert len(plan.align_tables) == 0


# ----------------- align path (diffing) -----------------


def test_align_add_drop_nullability_comments_props_and_pk_add():
    planner = Planner()
    desired = desired_table(
        cols=[
            Column("id", T.IntegerType(), is_nullable=False, comment="pk col"),
            Column("name", T.StringType(), is_nullable=True, comment="customer"),
        ],
        comment="hello",
        table_props={"delta.appendOnly": "true"},  # plus defaults via effective_table_properties
        pk_cols=["id"],
    )

    actual = table_state(
        columns=(
            ColumnState(
                "id", T.IntegerType(), is_nullable=True, comment=""
            ),  # needs tighten + comment
            ColumnState("legacy", T.StringType(), is_nullable=True, comment="old"),
        ),
        table_comment="",
        table_properties=MappingProxyType({}),  # no props yet
        pk=None,
    )

    snap = CatalogState(tables={"cat.sch.tbl": actual})
    plan = planner.plan([desired], snap)
    assert len(plan.create_tables) == 0
    assert len(plan.align_tables) == 1

    at: AlignTable = plan.align_tables[0]

    # additions: add 'name'
    assert at.add_columns == (ColumnAdd("name", T.StringType(), True, "customer"),)

    # drops: drop 'legacy'
    assert at.drop_columns == (ColumnDrop("legacy"),)

    # nullability: id -> NOT NULL
    assert at.change_nullability == (ColumnNullabilityChange("id", make_nullable=False),)

    # column comments: id changed from "" -> "pk col"
    assert isinstance(at.set_column_comments, SetColumnComments)
    assert at.set_column_comments.comments == {"id": "pk col", "name": "customer"}

    # table comment updated
    assert isinstance(at.set_table_comment, SetTableComment)
    assert at.set_table_comment.comment == "hello"

    # properties to set include at least delta.appendOnly (defaults may also be present)
    assert isinstance(at.set_table_properties, SetTableProperties)
    assert at.set_table_properties.properties["delta.appendOnly"] == "true"

    # PK should be added
    assert at.drop_primary_key is None
    assert isinstance(at.add_primary_key, PrimaryKeyAdd)
    assert at.add_primary_key.definition.columns == ("id",)


def test_align_noop_when_already_matches_emits_empty_align():
    planner = Planner()
    desired = desired_table(
        cols=[Column("id", T.IntegerType(), is_nullable=False)],
        comment="",
        table_props={},  # only defaults
        pk_cols=None,
    )
    actual = table_state(
        columns=(ColumnState("id", T.IntegerType(), is_nullable=False, comment=""),),
        table_comment="",
        table_properties=MappingProxyType({}),  # assume defaults not materialized in UC yet
        pk=None,
    )
    snap = CatalogState(tables={actual.full_name: actual})
    plan = planner.plan([desired], snap)
    at = plan.align_tables[0]
    props = str_keys(at.set_table_properties.properties)

    assert len(plan.align_tables) == 1
    assert at.add_columns == ()
    assert at.drop_columns == ()
    assert at.change_nullability == ()
    assert at.set_column_comments is None
    assert at.set_table_comment is None
    # Defaults may still need setting depending on UC; planner sets only diffs from actual
    assert at.set_table_properties is not None
    assert props.get("delta.columnMapping.mode") == "name"  # default being applied
    assert at.add_primary_key is None
    assert at.drop_primary_key is None


# ----------------- properties diffing -----------------


def test_property_updates_only_include_changed_keys_not_removals():
    planner = Planner()
    desired = desired_table(
        cols=[Column("id", T.IntegerType(), is_nullable=False)],
        table_props={"delta.appendOnly": "false"},  # planner iterates desired keys only
    )
    actual = table_state(
        columns=(ColumnState("id", T.IntegerType(), False, ""),),
        table_properties=MappingProxyType(
            {
                "delta.appendOnly": "true",
                "unmanaged.extra": "keep",
            }  # extra should NOT be removed by planner
        ),
    )
    snap = CatalogState(tables={"cat.sch.tbl": actual})
    at = planner.plan([desired], snap).align_tables[0]
    props = str_keys(at.set_table_properties.properties)

    assert isinstance(at.set_table_properties, SetTableProperties)
    assert props["delta.appendOnly"] == "false"
    assert "unmanaged.extra" not in props  # planner doesn’t remove extras, only sets diffs


# ----------------- PK logic -----------------


def test_pk_add_when_desired_exists_and_actual_none():
    planner = Planner()
    desired = desired_table(cols=[Column("id", T.IntegerType(), False)], pk_cols=["id"])
    actual = table_state(columns=(ColumnState("id", T.IntegerType(), False, ""),), pk=None)
    at = planner.plan([desired], CatalogState(tables={"cat.sch.tbl": actual})).align_tables[0]
    assert isinstance(at.add_primary_key, PrimaryKeyAdd)
    assert at.drop_primary_key is None


def test_pk_drop_when_actual_exists_and_desired_none():
    planner = Planner()
    desired = desired_table(cols=[Column("id", T.IntegerType(), False)], pk_cols=None)
    actual = table_state(
        columns=(ColumnState("id", T.IntegerType(), False, ""),),
        pk=PrimaryKeyState(name="pk_old", columns=("id",)),
    )
    at = planner.plan([desired], CatalogState(tables={"cat.sch.tbl": actual})).align_tables[0]
    assert isinstance(at.drop_primary_key, PrimaryKeyDrop)
    assert at.add_primary_key is None


def test_pk_recreate_when_columns_differ_or_order_differs_case_insensitive():
    planner = Planner()
    desired = desired_table(
        cols=[Column("id", T.IntegerType(), False), Column("name", T.StringType(), False)],
        pk_cols=["name", "id"],
    )
    # actual has same set different order and different case
    actual = table_state(
        columns=(
            ColumnState("id", T.IntegerType(), False, ""),
            ColumnState("name", T.StringType(), False, ""),
        ),
        pk=PrimaryKeyState(
            name="pk_old", columns=("ID", "NAME")
        ),  # same columns but different order than desired
    )
    at = planner.plan([desired], CatalogState(tables={"cat.sch.tbl": actual})).align_tables[0]
    assert isinstance(at.drop_primary_key, PrimaryKeyDrop)
    assert isinstance(at.add_primary_key, PrimaryKeyAdd)
    assert at.add_primary_key.definition.columns == ("name", "id")  # desired order


# ----------------- internal helpers (light checks) -----------------


def test_to_struct_builds_structtype_in_order():
    planner = Planner()
    st = planner._to_struct(
        [
            Column("a", T.StringType(), is_nullable=True),
            Column("b", T.IntegerType(), is_nullable=False),
        ]
    )
    assert [(f.name, type(f.dataType), f.nullable) for f in st.fields] == [
        ("a", T.StringType, True),
        ("b", T.IntegerType, False),
    ]


def test_compute_column_comment_updates_normalizes_and_includes_new_columns():
    planner = Planner()
    desired_cols = [
        Column("id", T.IntegerType(), is_nullable=False, comment=None),  # -> ""
        Column("name", T.StringType(), is_nullable=True, comment="cust"),  # -> "cust"
    ]
    actual_cols = [
        ColumnState(
            "id", T.IntegerType(), is_nullable=False, comment=""
        ),  # same after normalize -> no change
        # name absent -> treated as "" actual, so will be set to "cust"
    ]
    op = planner._compute_column_comment_updates(desired_cols, actual_cols)
    assert isinstance(op, SetColumnComments)
    assert op.comments == {"name": "cust"}


def test_table_comment_update_uses_normalization():
    planner = Planner()
    # None vs "" -> no change
    assert planner._compute_table_comment_update(None, "") is None
    assert planner._compute_table_comment_update("", None) is None
    # change when different after normalize
    sc = planner._compute_table_comment_update("x", "")
    assert isinstance(sc, SetTableComment)
    assert sc.comment == "x"

def test_compute_table_property_updates_returns_none_when_no_changes():
    planner = Planner()
    desired_props = {"delta.appendOnly": "false", "k": "v"}
    actual_props = {"delta.appendOnly": "false", "k": "v"}  # identical
    op = planner._compute_table_property_updates(desired_props, actual_props)
    assert op is None


def test_pk_helpers_noops_when_definitions_equal():
    planner = Planner()
    desired_def = PrimaryKeyDefinition(name="pk_x", columns=("id",))
    actual_state = PrimaryKeyState(name="anything", columns=("ID",))  # equal ignoring case

    # drop/add should both be None when equal
    assert planner._compute_primary_key_drop(desired_def, actual_state) is None
    assert planner._compute_primary_key_add(desired_def, actual_state) is None

    # and equality helper returns True
    assert planner._primary_key_definitions_equal(desired_def, actual_state)


def test_desired_pk_definition_none_when_absent():
    planner = Planner()
    t = Table(
        catalog_name="c",
        schema_name="s",
        table_name="t",
        columns=[Column("id", T.IntegerType(), is_nullable=False)],
        primary_key=None,
    )
    assert planner._desired_pk_definition(t) is None
