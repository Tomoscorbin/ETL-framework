import pytest
import pyspark.sql.types as T
from types import MappingProxyType
from collections.abc import Mapping

from src.delta_engine.models import Column, Table
from src.delta_engine.state.catalog_reader import CatalogReader
from src.delta_engine.state.states import ColumnState, PrimaryKeyState, TableState


# ---------------------------
# fakes for Spark/Delta
# ---------------------------

class FakeDF:
    def __init__(self, rows=None, columns=None, selected=None):
        self._rows = rows or []
        self.columns = columns or []
        self._selected = selected

    def collect(self):
        return list(self._rows)

    def take(self, n):
        return list(self._rows)[:n]

    def first(self):
        return self._rows[0] if self._rows else None

    def select(self, colname):
        # Return rows as dicts so row[colname] works like Spark Row
        return FakeDF(
            rows=[{colname: r.get(colname)} for r in self._rows],
            columns=[colname],
            selected=colname,
        )


class FakeDeltaTable:
    """Fake DeltaTable with the two calls we exercise: toDF().schema and detail()."""
    def __init__(self, struct: T.StructType, configuration: dict | None):
        self._struct = struct
        self._configuration = configuration  # dict or None

    def toDF(self):
        class _DF:
            schema = self._struct
        return _DF()

    def detail(self):
        # In real Spark this returns a one-row DF with a `configuration` column (a map)
        cols = ["configuration"]
        rows = [{"configuration": self._configuration}]
        return FakeDF(rows=rows, columns=cols)


class FakeSparkCatalog:
    def __init__(self, existing: set[str], comments_by_table: dict[str, str], column_descriptions: dict[str, dict[str, str]]):
        self._existing = existing
        self._comments_by_table = comments_by_table               # table -> comment
        self._column_descriptions = column_descriptions           # table -> {col_name: description}

    def tableExists(self, full_name_unescaped: str) -> bool:
        return full_name_unescaped in self._existing

    def getTable(self, full_name_unescaped: str):
        class _Tbl:
            description = self._comments_by_table.get(full_name_unescaped, "")
        return _Tbl()

    def listColumns(self, full_name_unescaped: str):
        # Return objects with .name and .description; accept mixed-case keys
        class _Col:
            def __init__(self, name, description): self.name, self.description = name, description
        descs = self._column_descriptions.get(full_name_unescaped, {})
        return [_Col(name, desc) for name, desc in descs.items()]


class FakeSpark:
    def __init__(self, catalog: FakeSparkCatalog):
        self.catalog = catalog

    def sql(self, _sql: str) -> FakeDF:
        # The test will monkeypatch CatalogReader._run / _take_first_value or
        # monkeypatch this method to return the DF it wants.
        return FakeDF(rows=[], columns=[])


# ---------------------------
# Fixtures
# ---------------------------

@pytest.fixture
def reader(monkeypatch):
    # Default: one existing table with a simple schema, no PK
    full = "cat.sch.tbl"
    catalog = FakeSparkCatalog(
        existing={full},
        comments_by_table={full: "hello"},
        column_descriptions={full: {"ID": "surrogate key", "name": "customer name"}},  # mixed-case to test case-insensitivity
    )
    spark = FakeSpark(catalog)

    r = CatalogReader(spark)

    # Monkeypatch the seam CatalogReader._read_table to return our FakeDeltaTable
    schema = T.StructType([
        T.StructField("id", T.IntegerType(), nullable=False),
        T.StructField("name", T.StringType(), nullable=True),
    ])

    def fake_read_table(self, _full):
        return FakeDeltaTable(struct=schema, configuration={"delta.appendOnly": "false", "x": "y"})

    monkeypatch.setattr(CatalogReader, "_read_table", fake_read_table, raising=True)
    return r


# ---------------------------
# Tests
# ---------------------------

def test_snapshot_keys_by_full_name_and_reads_each_table(reader):
    models = [
        Table(catalog_name="cat", schema_name="sch", table_name="tbl",
              columns=[Column("id", T.IntegerType(), is_nullable=False)], primary_key=None),
        Table(catalog_name="cat", schema_name="sch", table_name="missing",
              columns=[Column("id", T.IntegerType(), is_nullable=False)], primary_key=None),
    ]

    # Make "missing" actually missing
    reader.spark.catalog._existing.discard("cat.sch.missing")

    snap = reader.snapshot(models)
    assert set(snap.tables.keys()) == {"cat.sch.tbl", "cat.sch.missing"}
    assert snap.tables["cat.sch.tbl"].exists is True
    assert snap.tables["cat.sch.missing"].exists is False


def test_read_table_state_nonexistent_uses_empty_factory(reader):
    model = Table("cat", "sch", "missing", columns=[Column("id", T.IntegerType(), is_nullable=False)])
    ts = reader._read_table_state(model)
    assert ts.full_name == "cat.sch.missing"
    assert ts.exists is False
    assert ts.columns == ()
    assert ts.table_comment == ""
    assert dict(ts.table_properties) == {}  # read-only mapping, but empty
    assert ts.primary_key is None


def test_read_table_state_existing_merges_schema_comments_properties_and_pk(reader, monkeypatch):
    # Simulate PK present via the internal helpers
    monkeypatch.setattr(reader, "_read_primary_key_name_for_table", lambda _: "pk_tbl")
    monkeypatch.setattr(reader, "_read_primary_key_columns_for_table", lambda _: ["id"])

    model = Table("cat", "sch", "tbl", columns=[Column("id", T.IntegerType(), is_nullable=False)])
    ts = reader._read_table_state(model)

    # Schema
    assert [c.name for c in ts.columns] == ["id", "name"]
    assert ts.columns[0].comment == "surrogate key"     # from catalog, case-insensitive merge
    assert ts.columns[1].comment == "customer name"

    # Table metadata
    assert ts.table_comment == "hello"
    assert ts.table_properties["delta.appendOnly"] == "false"
    assert ts.table_properties["x"] == "y"

    # PK
    assert isinstance(ts.primary_key, PrimaryKeyState)
    assert ts.primary_key.name == "pk_tbl"
    assert ts.primary_key.columns == ("id",)


def test_read_column_comments_lower_is_case_insensitive(reader):
    out = reader._read_column_comments_lower("cat.sch.tbl")
    # keys should be lower-cased regardless of catalog case
    assert out == {"id": "surrogate key", "name": "customer name"}


def test_merge_struct_and_comments(reader):
    struct = T.StructType([
        T.StructField("ID", T.IntegerType(), False),
        T.StructField("Name", T.StringType(), True),
    ])
    comments = {"id": "ID comment", "name": "Name comment"}
    merged = reader._merge_struct_and_comments(struct, comments)
    assert merged == (
        ColumnState(name="ID", data_type=T.IntegerType(), is_nullable=False, comment="ID comment"),
        ColumnState(name="Name", data_type=T.StringType(), is_nullable=True, comment="Name comment"),
    )


def test_read_table_comment_swallows_errors(reader, monkeypatch):
    # Force getTable to raise
    def boom(_name):
        raise RuntimeError("perm issue")
    reader.spark.catalog.getTable = boom  # type: ignore[method-assign]
    assert reader._read_table_comment("cat.sch.tbl") == ""


def test_read_table_properties_handles_missing_configuration_column(reader):
    # A Delta with no 'configuration' column in detail()
    class NoConfigDelta(FakeDeltaTable):
        def detail(self):
            return FakeDF(rows=[{"other": 1}], columns=["other"])

    delta = NoConfigDelta(struct=T.StructType([]), configuration=None)
    props = reader._read_table_properties(delta)
    assert isinstance(props, Mapping)
    assert len(props) == 0  # empty, read-only mapping


def test_read_table_properties_returns_readonly_mapping(reader):
    delta = reader._read_table("cat.sch.tbl")
    props = reader._read_table_properties(delta)
    assert isinstance(props, Mapping)
    assert isinstance(props, MappingProxyType)
    assert props["delta.appendOnly"] == "false"
    assert props["x"] == "y"
    with pytest.raises(TypeError):
        props["new"] = "nope"


def test_read_primary_key_state_none_when_no_pk(reader, monkeypatch):
    monkeypatch.setattr(reader, "_read_primary_key_name_for_table", lambda _: None)
    assert reader._read_primary_key_state(("c", "s", "t")) is None


def test_read_primary_key_state_orders_columns(reader, monkeypatch):
    monkeypatch.setattr(reader, "_read_primary_key_name_for_table", lambda _: "pk_x")
    monkeypatch.setattr(reader, "_read_primary_key_columns_for_table", lambda _: ["b", "a"])
    pk = reader._read_primary_key_state(("c", "s", "t"))
    assert pk.name == "pk_x"
    assert pk.columns == ("b", "a")  # order is whatever _read_primary_key_columns_for_table returns


def test_read_primary_key_columns_for_table_defensive_sort(reader, monkeypatch):
    # Monkeypatch _run to return out-of-order rows with ordinal_position
    rows = [
        {"column_name": "b", "ordinal_position": 2},
        {"column_name": "a", "ordinal_position": 1},
    ]
    monkeypatch.setattr(reader, "_run", lambda _sql: rows)
    cols = reader._read_primary_key_columns_for_table(("c", "s", "t"))
    assert cols == ["a", "b"]


def test_take_first_value_handles_empty_and_nonempty(reader):
    # Provide a fake spark.sql that returns a DF with take()
    def fake_sql(_sql):
        return FakeDF(rows=[{"name": "pk_tbl"}], columns=["name"])
    reader.spark.sql = fake_sql  # type: ignore[method-assign]

    assert reader._take_first_value("ignored", "name") == "pk_tbl"

    def fake_sql_empty(_sql):
        return FakeDF(rows=[], columns=["name"])
    reader.spark.sql = fake_sql_empty  # type: ignore[method-assign]
    assert reader._take_first_value("ignored", "name") is None


# ---------------------------
# Extra: direct TableState test
# ---------------------------

def test_tablestate_defaults_and_readonly_properties():
    ts = TableState(catalog_name="c", schema_name="s", table_name="t", exists=True)
    assert ts.full_name == "c.s.t"
    assert isinstance(ts.table_properties, MappingProxyType)
    with pytest.raises(TypeError):
        ts.table_properties["x"] = "y"  # read-only
