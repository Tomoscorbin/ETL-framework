from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

import pyspark.sql.types as T
import pytest

import src.delta_engine.state.catalog_reader as cr  # module under test
from src.delta_engine.models import Column, Table
from src.delta_engine.state.catalog_reader import CatalogReader
from src.delta_engine.state.states import ColumnState, PrimaryKeyState, TableState

# ---------------------------
# fakes for Spark/Delta
# ---------------------------


class FakeDF:
    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        columns: list[str] | None = None,
        selected: str | None = None,
    ) -> None:
        self._rows: list[dict[str, Any]] = rows or []
        self.columns: list[str] = columns or []
        self._selected: str | None = selected

    def collect(self) -> list[dict[str, Any]]:
        return list(self._rows)

    def take(self, n: int) -> list[dict[str, Any]]:
        return list(self._rows)[:n]

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None

    def select(self, colname: str) -> FakeDF:
        # Return rows as dicts so row[colname] works like Spark Row
        return FakeDF(
            rows=[{colname: r.get(colname)} for r in self._rows],
            columns=[colname],
            selected=colname,
        )


class FakeDeltaTable:
    """Fake DeltaTable with the two calls we exercise: toDF().schema and detail()."""

    def __init__(self, struct: T.StructType, configuration: dict[str, str] | None) -> None:
        self._struct = struct
        self._configuration = configuration  # dict or None

    def toDF(self) -> Any:
        class _DF:
            schema = self._struct  # type: ignore[attr-defined]

        return _DF()

    def detail(self) -> FakeDF:
        # In real Spark this returns a one-row DF with a `configuration` column (a map)
        cols = ["configuration"]
        rows = [{"configuration": self._configuration}]
        return FakeDF(rows=rows, columns=cols)


class FakeSparkCatalog:
    def __init__(
        self,
        existing: set[str],
        comments_by_table: dict[str, str],
        column_descriptions: dict[str, dict[str, str]],
    ) -> None:
        self._existing = existing
        self._comments_by_table = comments_by_table  # table -> comment
        self._column_descriptions = column_descriptions  # table -> {col_name: description}

    def tableExists(self, full_name_unescaped: str) -> bool:  # noqa: N802 (Spark API)
        return full_name_unescaped in self._existing

    def getTable(self, full_name_unescaped: str) -> Any:  # noqa: N802 (Spark API)
        class _Tbl:
            description = self._comments_by_table.get(full_name_unescaped, "")

        return _Tbl()

    def listColumns(self, full_name_unescaped: str) -> list[Any]:  # noqa: N802 (Spark API)
        # Return objects with .name and .description; accept mixed-case keys
        class _Col:
            def __init__(self, name: str, description: str) -> None:
                self.name, self.description = name, description

        descs = self._column_descriptions.get(full_name_unescaped, {})
        return [_Col(name, desc) for name, desc in descs.items()]


class FakeSpark:
    def __init__(self, catalog: FakeSparkCatalog) -> None:
        self.catalog = catalog

    def sql(self, _sql: str) -> FakeDF:
        # Tests override this when needed
        return FakeDF(rows=[], columns=[])


# ---------------------------
# Fixture
# ---------------------------


@pytest.fixture
def reader(monkeypatch: pytest.MonkeyPatch) -> CatalogReader:
    # Default: one existing table with a simple schema, no PK
    full = "cat.sch.tbl"
    catalog = FakeSparkCatalog(
        existing={full},
        comments_by_table={full: "hello"},
        column_descriptions={full: {"ID": "surrogate key", "name": "customer name"}},  # mixed-case
    )
    spark = FakeSpark(catalog)
    r = CatalogReader(spark)

    # Schema for the fake table
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), nullable=False),
            T.StructField("name", T.StringType(), nullable=True),
        ]
    )

    # Patch DeltaTable used inside the module under test
    class DeltaTableStub:
        @staticmethod
        def forName(_spark: Any, _full: str) -> FakeDeltaTable:
            return FakeDeltaTable(
                struct=schema,
                configuration={"delta.appendOnly": "false", "x": "y"},
            )

    monkeypatch.setattr(cr, "DeltaTable", DeltaTableStub, raising=True)
    return r


# ---------------------------
# Tests
# ---------------------------


def test_snapshot_keys_by_full_name_and_reads_each_table(reader: CatalogReader) -> None:
    models = [
        Table(
            catalog_name="cat",
            schema_name="sch",
            table_name="tbl",
            columns=[Column("id", T.IntegerType(), is_nullable=False)],
            primary_key=None,
        ),
        Table(
            catalog_name="cat",
            schema_name="sch",
            table_name="missing",
            columns=[Column("id", T.IntegerType(), is_nullable=False)],
            primary_key=None,
        ),
    ]

    # Make "missing" actually missing
    reader.spark.catalog._existing.discard("cat.sch.missing")

    snap = reader.snapshot(models)
    assert set(snap.tables.keys()) == {"cat.sch.tbl", "cat.sch.missing"}
    assert snap.tables["cat.sch.tbl"].exists is True
    assert snap.tables["cat.sch.missing"].exists is False


def test_read_table_state_nonexistent_uses_empty_factory(reader: CatalogReader) -> None:
    model = Table(
        "cat", "sch", "missing", columns=[Column("id", T.IntegerType(), is_nullable=False)]
    )
    ts = reader._read_table_state(model)
    assert ts.full_name == "cat.sch.missing"
    assert ts.exists is False
    assert ts.columns == ()
    assert ts.table_comment == ""
    assert dict(ts.table_properties) == {}  # read-only mapping, but empty
    assert ts.primary_key is None


def test_read_table_state_existing_merges_schema_comments_properties_and_pk(
    reader: CatalogReader, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Simulate PK present via the internal helpers
    monkeypatch.setattr(
        reader, "_read_primary_key_name_for_table", lambda _tpn: "pk_tbl", raising=True
    )
    monkeypatch.setattr(
        reader, "_read_primary_key_columns_for_table", lambda _tpn: ["id"], raising=True
    )

    model = Table("cat", "sch", "tbl", columns=[Column("id", T.IntegerType(), is_nullable=False)])
    ts = reader._read_table_state(model)

    # Schema
    assert [c.name for c in ts.columns] == ["id", "name"]
    assert ts.columns[0].comment == "surrogate key"  # from catalog, case-insensitive merge
    assert ts.columns[1].comment == "customer name"

    # Table metadata
    assert ts.table_comment == "hello"
    assert ts.table_properties["delta.appendOnly"] == "false"
    assert ts.table_properties["x"] == "y"

    # PK
    assert isinstance(ts.primary_key, PrimaryKeyState)
    assert ts.primary_key.name == "pk_tbl"
    assert ts.primary_key.columns == ("id",)


def test_read_column_comments_lower_is_case_insensitive(reader: CatalogReader) -> None:
    out = reader._read_column_comments_lower("cat.sch.tbl")
    # keys should be lower-cased regardless of catalog case
    assert out == {"id": "surrogate key", "name": "customer name"}


def test_merge_struct_and_comments(reader: CatalogReader) -> None:
    struct = T.StructType(
        [
            T.StructField("ID", T.IntegerType(), False),
            T.StructField("Name", T.StringType(), True),
        ]
    )
    comments = {"id": "ID comment", "name": "Name comment"}
    merged = reader._merge_struct_and_comments(struct, comments)
    assert merged == (
        ColumnState(name="ID", data_type=T.IntegerType(), is_nullable=False, comment="ID comment"),
        ColumnState(
            name="Name", data_type=T.StringType(), is_nullable=True, comment="Name comment"
        ),
    )


def test_read_table_comment_swallows_errors(reader: CatalogReader) -> None:
    # Force getTable to raise
    def boom(_name: str) -> Any:
        raise RuntimeError("perm issue")

    reader.spark.catalog.getTable = boom  # type: ignore[method-assign]
    assert reader._read_table_comment("cat.sch.tbl") == ""


def test_read_table_properties_handles_missing_configuration_column(reader: CatalogReader) -> None:
    # A Delta with no 'configuration' column in detail()
    class NoConfigDelta(FakeDeltaTable):
        def detail(self) -> FakeDF:
            return FakeDF(rows=[{"other": 1}], columns=["other"])

    delta = NoConfigDelta(struct=T.StructType([]), configuration=None)
    props = reader._read_table_properties(delta)
    assert isinstance(props, Mapping)
    assert len(props) == 0  # empty, read-only mapping


def test_read_table_properties_returns_readonly_mapping(reader: CatalogReader) -> None:
    delta = reader._read_table("cat.sch.tbl")
    props = reader._read_table_properties(delta)
    assert isinstance(props, Mapping)
    assert isinstance(props, MappingProxyType)
    assert props["delta.appendOnly"] == "false"
    assert props["x"] == "y"
    with pytest.raises(TypeError):
        props["new"] = "nope"  # type: ignore[index]


def test_read_primary_key_state_none_when_no_pk(
    reader: CatalogReader, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(reader, "_read_primary_key_name_for_table", lambda _tpn: None, raising=True)
    assert reader._read_primary_key_state(("c", "s", "t")) is None


def test_read_primary_key_state_orders_columns(
    reader: CatalogReader, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        reader, "_read_primary_key_name_for_table", lambda _tpn: "pk_x", raising=True
    )
    monkeypatch.setattr(
        reader, "_read_primary_key_columns_for_table", lambda _tpn: ["b", "a"], raising=True
    )
    pk = reader._read_primary_key_state(("c", "s", "t"))
    assert pk is not None
    assert pk.name == "pk_x"
    assert pk.columns == ("b", "a")  # order is whatever _read_primary_key_columns_for_table returns


def test_read_primary_key_columns_for_table_defensive_sort(
    reader: CatalogReader, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Monkeypatch _run to return out-of-order rows with ordinal_position
    rows = [
        {"column_name": "b", "ordinal_position": 2},
        {"column_name": "a", "ordinal_position": 1},
    ]
    monkeypatch.setattr(reader, "_run", lambda _sql: rows, raising=True)
    cols = reader._read_primary_key_columns_for_table(("c", "s", "t"))
    assert cols == ["a", "b"]


def test_take_first_value_handles_empty_and_nonempty(reader: CatalogReader) -> None:
    # Provide a fake spark.sql that returns a DF with take()
    def fake_sql(_sql: str) -> FakeDF:
        return FakeDF(rows=[{"name": "pk_tbl"}], columns=["name"])

    reader.spark.sql = fake_sql  # type: ignore[method-assign]
    assert reader._take_first_value("ignored", "name") == "pk_tbl"

    def fake_sql_empty(_sql: str) -> FakeDF:
        return FakeDF(rows=[], columns=["name"])

    reader.spark.sql = fake_sql_empty  # type: ignore[method-assign]
    assert reader._take_first_value("ignored", "name") is None


def test_read_table_properties_config_column_but_no_rows_returns_empty(
    reader: CatalogReader,
) -> None:
    class NoRowsDelta(FakeDeltaTable):
        def detail(self) -> FakeDF:
            # 'configuration' column present, but 0 rows
            return FakeDF(rows=[], columns=["configuration"])

    props = reader._read_table_properties(NoRowsDelta(struct=T.StructType([]), configuration=None))
    assert isinstance(props, MappingProxyType)
    assert dict(props) == {}


def test_read_table_properties_config_value_none_returns_empty(reader: CatalogReader) -> None:
    class NoneValueDelta(FakeDeltaTable):
        def detail(self) -> FakeDF:
            # 1 row, but configuration is None
            return FakeDF(rows=[{"configuration": None}], columns=["configuration"])

    props = reader._read_table_properties(
        NoneValueDelta(struct=T.StructType([]), configuration=None)
    )
    assert isinstance(props, MappingProxyType)
    assert dict(props) == {}


def test_read_table_properties_exception_returns_empty(reader: CatalogReader) -> None:
    class BoomDelta(FakeDeltaTable):
        def detail(self) -> FakeDF:
            raise RuntimeError("boom")

    props = reader._read_table_properties(BoomDelta(struct=T.StructType([]), configuration=None))
    assert isinstance(props, MappingProxyType)
    assert dict(props) == {}


def test_tablestate_defaults_and_readonly_properties() -> None:
    ts = TableState(catalog_name="c", schema_name="s", table_name="t", exists=True)
    assert ts.full_name == "c.s.t"
    assert isinstance(ts.table_properties, MappingProxyType)
    with pytest.raises(TypeError):
        ts.table_properties["x"] = "y"  # type: ignore[index]
