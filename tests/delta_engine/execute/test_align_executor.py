import pyspark.sql.types as T
from collections import OrderedDict

from src.delta_engine.execute.align_executor import AlignExecutor
from src.delta_engine.actions import (
    AlignTable,
    ColumnAdd,
    ColumnDrop,
    ColumnNullabilityChange,
    PrimaryKeyAdd,
    PrimaryKeyDefinition,
    PrimaryKeyDrop,
    SetColumnComments,
    SetTableComment,
    SetTableProperties,
)


class RecordingDDL:
    """Fake DeltaDDL that records all calls in order."""
    def __init__(self):
        self.calls = []

    def drop_primary_key(self, table, name):
        self.calls.append(("drop_primary_key", table, name))

    def add_column(self, table, name, dtype, comment):
        # store dtype’s simpleString for easy equality checks
        self.calls.append(("add_column", table, name, dtype.simpleString(), comment))

    def drop_columns(self, table, names):
        self.calls.append(("drop_columns", table, list(names)))

    def set_column_nullability(self, table, name, make_nullable):
        self.calls.append(("set_column_nullability", table, name, make_nullable))

    def add_primary_key(self, table, name, columns):
        self.calls.append(("add_primary_key", table, name, list(columns)))

    def set_column_comment(self, table, name, comment):
        self.calls.append(("set_column_comment", table, name, comment))

    def set_table_comment(self, table, comment):
        self.calls.append(("set_table_comment", table, comment))

    def set_table_properties(self, table, props):
        self.calls.append(("set_table_properties", table, dict(props)))


class FakeSpark:
    pass


def make_align_full():
    """Build an AlignTable with all steps populated."""
    return AlignTable(
        catalog_name="cat",
        schema_name="sch",
        table_name="tbl",
        # 1) adds (two, order matters)
        add_columns=(
            ColumnAdd("age", T.IntegerType(), True, "years"),
            ColumnAdd("name", T.StringType(), True, "customer"),
        ),
        # 2) drops
        drop_columns=(ColumnDrop("legacy"),),
        # 3) nullability changes
        change_nullability=(ColumnNullabilityChange("id", make_nullable=False),),
        # 0) drop PK, 4) add PK
        drop_primary_key=PrimaryKeyDrop("pk_old"),
        add_primary_key=PrimaryKeyAdd(PrimaryKeyDefinition("pk_new", ("id",))),
        # 5) column comments (preserve insertion order)
        set_column_comments=SetColumnComments(OrderedDict([("age", "years"), ("name", "customer")])),
        # 6) table comment
        set_table_comment=SetTableComment("updated"),
        # 7) properties
        set_table_properties=SetTableProperties({"delta.appendOnly": "false"}),
    )


def test_apply_calls_steps_in_documented_order():
    runner = AlignExecutor(FakeSpark())
    ddl = RecordingDDL()
    runner.ddl = ddl  # inject recorder

    action = make_align_full()
    runner.apply(action)

    expected = [
        ("drop_primary_key", "cat.sch.tbl", "pk_old"),
        ("add_column", "cat.sch.tbl", "age", "int", "years"),
        ("add_column", "cat.sch.tbl", "name", "string", "customer"),
        ("drop_columns", "cat.sch.tbl", ["legacy"]),
        ("set_column_nullability", "cat.sch.tbl", "id", False),
        ("add_primary_key", "cat.sch.tbl", "pk_new", ["id"]),
        ("set_column_comment", "cat.sch.tbl", "age", "years"),
        ("set_column_comment", "cat.sch.tbl", "name", "customer"),
        ("set_table_comment", "cat.sch.tbl", "updated"),
        ("set_table_properties", "cat.sch.tbl", {"delta.appendOnly": "false"}),
    ]
    assert ddl.calls == expected

    # All calls must use the same fully-qualified table string
    assert all(call[1] == "cat.sch.tbl" for call in ddl.calls)


def test_apply_skips_optional_steps_when_absent():
    runner = AlignExecutor(FakeSpark())
    ddl = RecordingDDL()
    runner.ddl = ddl

    # Only add one column; everything else empty/None
    action = AlignTable(
        catalog_name="cat",
        schema_name="sch",
        table_name="tbl",
        add_columns=(ColumnAdd("c", T.StringType(), True, ""),),
        drop_columns=(),
        change_nullability=(),
        set_column_comments=None,
        set_table_comment=None,
        set_table_properties=None,
        drop_primary_key=None,
        add_primary_key=None,
    )

    runner.apply(action)

    assert ddl.calls == [
        ("add_column", "cat.sch.tbl", "c", "string", ""),
    ]


def test_apply_passes_exact_args_to_ddl():
    """Spot-check a few argument shapes and values."""
    runner = AlignExecutor(FakeSpark())
    ddl = RecordingDDL()
    runner.ddl = ddl

    action = AlignTable(
        catalog_name="cat",
        schema_name="sch",
        table_name="tbl",
        add_columns=(ColumnAdd("n", T.DecimalType(10, 2), True, "price"),),
        drop_columns=(ColumnDrop("old"),),
        change_nullability=(ColumnNullabilityChange("flag", True),),
        set_column_comments=SetColumnComments({"n": "price"}),
        set_table_comment=SetTableComment("tbl note"),
        set_table_properties=SetTableProperties({"k": "v"}),
        drop_primary_key=PrimaryKeyDrop("pk_x"),
        add_primary_key=PrimaryKeyAdd(PrimaryKeyDefinition("pk_y", ("n", "flag"))),
    )

    runner.apply(action)

    assert ("add_column", "cat.sch.tbl", "n", "decimal(10,2)", "price") in ddl.calls
    assert ("drop_columns", "cat.sch.tbl", ["old"]) in ddl.calls
    assert ("set_column_nullability", "cat.sch.tbl", "flag", True) in ddl.calls
    assert ("add_primary_key", "cat.sch.tbl", "pk_y", ["n", "flag"]) in ddl.calls
    assert ("set_column_comment", "cat.sch.tbl", "n", "price") in ddl.calls
    assert ("set_table_comment", "cat.sch.tbl", "tbl note") in ddl.calls
    assert ("set_table_properties", "cat.sch.tbl", {"k": "v"}) in ddl.calls
