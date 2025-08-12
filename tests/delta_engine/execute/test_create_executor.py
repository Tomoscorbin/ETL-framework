# tests/delta_engine/execute/test_create_executor.py
import pyspark.sql.types as T
import pytest

from src.delta_engine.execute.create_executor import CreateExecutor
from src.delta_engine.actions import CreateTable, PrimaryKeyDefinition


# --- fakes ---

class RecordingDDL:
    def __init__(self, spark):
        self.spark = spark
        self.calls = []

    def create_table_if_not_exists(self, table, schema_struct, table_comment):
        # keep schema fields minimal for assertions
        fields = [(f.name, f.dataType.simpleString(), f.nullable) for f in schema_struct.fields]
        self.calls.append(("create", table, tuple(fields), table_comment))

    def set_table_properties(self, table, props):
        self.calls.append(("props", table, dict(props)))

    def set_column_comment(self, table, column, comment):
        self.calls.append(("col_comment", table, column, comment))

    def add_primary_key(self, table, name, columns):
        self.calls.append(("pk", table, name, list(columns)))


class FakeSpark:
    pass


def make_schema():
    return T.StructType([
        T.StructField("id", T.IntegerType(), nullable=False),
        T.StructField("name", T.StringType(), nullable=True),
    ])


def make_create(**overrides) -> CreateTable:
    base = dict(
        catalog_name="cat",
        schema_name="sch",
        table_name="tbl",
        schema_struct=make_schema(),
        table_comment="hello",
        table_properties={"delta.appendOnly": "false"},
        column_comments={"id": "primary key", "name": ""},
        primary_key=PrimaryKeyDefinition(name="pk_cat_sch_tbl__id", columns=("id",)),
    )
    base.update(overrides)
    return CreateTable(**base)


# --- tests ---

def test_constructor_wires_spark_into_DeltaDDL(monkeypatch):
    created = {}
    def ddl_factory(spark):
        created["ddl"] = RecordingDDL(spark)
        return created["ddl"]

    # patch the class in the module under test
    import src.delta_engine.execute.create_executor as create_mod
    monkeypatch.setattr(create_mod, "DeltaDDL", ddl_factory)

    spark = FakeSpark()
    runner = CreateExecutor(spark)

    assert created["ddl"].spark is spark
    assert runner._ddl is created["ddl"]


def test_apply_calls_steps_in_order_and_with_right_args():
    spark = FakeSpark()
    runner = CreateExecutor(spark)
    recorder = RecordingDDL(spark)
    runner._ddl = recorder  # inject

    action = make_create()
    runner.apply(action)

    assert recorder.calls == [
        ("create", "cat.sch.tbl",
         (("id", "int", False), ("name", "string", True)),
         "hello"),
        ("props", "cat.sch.tbl", {"delta.appendOnly": "false"}),
        ("col_comment", "cat.sch.tbl", "id", "primary key"),
        ("pk", "cat.sch.tbl", "pk_cat_sch_tbl__id", ["id"]),
    ]


def test_apply_noops_for_empty_props_no_comments_no_pk():
    spark = FakeSpark()
    runner = CreateExecutor(spark)
    recorder = RecordingDDL(spark)
    runner._ddl = recorder

    action = make_create(
        table_properties={},               # falsy -> skip
        column_comments={"id": ""},        # only empty -> skip
        primary_key=None,                  # skip
        table_comment="",                  # allowed; still passed to create
    )
    runner.apply(action)

    assert recorder.calls == [
        ("create", "cat.sch.tbl",
         (("id", "int", False), ("name", "string", True)),
         ""),  # empty comment still passed on creation
    ]


def test_apply_sets_only_non_empty_column_comments():
    spark = FakeSpark()
    runner = CreateExecutor(spark)
    recorder = RecordingDDL(spark)
    runner._ddl = recorder

    action = make_create(column_comments={"id": "", "name": "customer"})
    runner.apply(action)

    # ensure only the non-empty one was set
    assert ("col_comment", "cat.sch.tbl", "name", "customer") in recorder.calls
    assert all(not (c[0] == "col_comment" and c[2] == "id") for c in recorder.calls)


def test_apply_primary_key_columns_order_is_preserved():
    spark = FakeSpark()
    runner = CreateExecutor(spark)
    recorder = RecordingDDL(spark)
    runner._ddl = recorder

    action = make_create(primary_key=PrimaryKeyDefinition("pk_custom", ("name", "id")))
    runner.apply(action)

    assert ("pk", "cat.sch.tbl", "pk_custom", ["name", "id"]) in recorder.calls
