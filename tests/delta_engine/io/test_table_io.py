import pyspark.sql.types as T

import src.delta_engine.io.table_io as table_io_mod  # for monkeypatching LOGGER
from src.delta_engine.io.table_io import TableIO
from src.delta_engine.models import Column, Table

# --------- fakes ---------


class FakeWrite:
    def __init__(self):
        self.calls = []

    def saveAsTable(self, *, name, mode, **kwargs):
        fmt = kwargs.get("format")
        self.calls.append((name, mode, fmt))


class FakeDataFrame:
    def __init__(self):
        self.selected = None
        self.write = FakeWrite()

    def select(self, cols):
        self.selected = list(cols)
        return self


class FakeSpark:
    def __init__(self):
        self.last_table_asked = None
        self._table_return = None

    def table(self, full_name):
        self.last_table_asked = full_name
        return self._table_return


class FakeLogger:
    def __init__(self):
        self.messages = []

    def info(self, msg):
        self.messages.append(msg)


# --------- helpers ---------


def make_table():
    return Table(
        catalog_name="cat",
        schema_name="sch",
        table_name="tbl",
        columns=[
            Column("id", T.IntegerType(), is_nullable=False),
            Column("name", T.StringType(), comment="cust name"),
            Column("created_at", T.TimestampType(), is_nullable=True),
        ],
        comment="customer dim",
    )


# --------- tests ---------


def test_read_uses_fully_qualified_name_and_returns_df():
    spark = FakeSpark()
    expected_df = FakeDataFrame()
    spark._table_return = expected_df

    tio = TableIO(spark)
    table = make_table()

    df = tio.read(table)

    assert spark.last_table_asked == "cat.sch.tbl"
    assert df is expected_df


def test_overwrite_selects_model_columns_writes_overwrite_delta_and_logs(monkeypatch):
    # patch LOGGER in the module under test
    fake_logger = FakeLogger()
    monkeypatch.setattr(table_io_mod, "LOGGER", fake_logger, raising=True)

    spark = FakeSpark()
    tio = TableIO(spark)
    table = make_table()

    df = FakeDataFrame()
    tio.overwrite(table, df)

    # Selected columns must match model order exactly
    assert df.selected == ["id", "name", "created_at"]

    # One write call with correct params
    assert df.write.calls == [("cat.sch.tbl", "overwrite", "delta")]

    # Log message contains the fully qualified table name
    assert fake_logger.messages == ["Table cat.sch.tbl overwritten."]
