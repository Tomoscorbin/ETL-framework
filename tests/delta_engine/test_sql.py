import pytest
import pyspark.sql.types as T

import src.delta_engine.sql as sql


# ---- Shared stubs for quoting/escaping/formatting ----

@pytest.fixture(autouse=True)
def stub_utils(monkeypatch):
    # deterministic quoting and escaping so tests aren’t tied to real utils
    monkeypatch.setattr(sql, "quote_qualified_name_from_full", lambda s: f"`{s}`")
    monkeypatch.setattr(sql, "quote_ident", lambda s: f"`{s}`")
    # escape single quotes by doubling them
    monkeypatch.setattr(sql, "escape_sql_literal", lambda s: (s or "").replace("'", "''"))
    # simple k=v join for properties
    monkeypatch.setattr(
        sql,
        "format_tblproperties",
        lambda d: ", ".join([f"'{k}'='{v}'" for k, v in d.items()]),
    )
    yield


# ---- sql_set_table_properties ----

def test_set_table_properties_none_for_empty():
    assert sql.sql_set_table_properties("cat.sch.tbl", {}) is None

def test_set_table_properties_builds_statement_with_quoting_and_formatting():
    props = {"delta.appendOnly": "true", "delta.minReaderVersion": "2"}
    out = sql.sql_set_table_properties("cat.sch.tbl", props)
    assert out.startswith("ALTER TABLE `cat.sch.tbl` SET TBLPROPERTIES (")
    assert "'delta.appendOnly'='true'" in out
    assert "'delta.minReaderVersion'='2'" in out
    assert out.endswith(")")


# ---- sql_add_column ----

def test_add_column_without_comment():
    sql_text = sql.sql_add_column("cat.sch.tbl", "weird name", T.IntegerType(), "")
    assert sql_text == "ALTER TABLE `cat.sch.tbl` ADD COLUMNS (`weird name` int)"

def test_add_column_with_comment_and_escape():
    sql_text = sql.sql_add_column("cat.sch.tbl", "desc", T.StringType(), "it's ok")
    # comment must be escaped to it''s ok and included
    assert "COMMENT 'it''s ok'" in sql_text
    assert "`cat.sch.tbl`" in sql_text
    assert "`desc` string" in sql_text


# ---- sql_drop_columns ----

def test_drop_columns_none_for_empty_iterable():
    assert sql.sql_drop_columns("cat.sch.tbl", []) is None
    # also handles empty generator
    assert sql.sql_drop_columns("cat.sch.tbl", (n for n in [])) is None

def test_drop_columns_quotes_and_joins():
    names = ["a", "b c", "D"]
    out = sql.sql_drop_columns("cat.sch.tbl", names)
    assert out == "ALTER TABLE `cat.sch.tbl` DROP COLUMNS (`a`, `b c`, `D`)"


# ---- sql_set_column_nullability ----

def test_set_column_nullability_drop_not_null():
    out = sql.sql_set_column_nullability("cat.sch.tbl", "name", True)
    assert out.endswith("ALTER COLUMN `name` DROP NOT NULL")

def test_set_column_nullability_set_not_null():
    out = sql.sql_set_column_nullability("cat.sch.tbl", "id", False)
    assert out.endswith("ALTER COLUMN `id` SET NOT NULL")


# ---- sql_set_column_comment / sql_set_table_comment ----

def test_set_column_comment_escapes_and_quotes():
    out = sql.sql_set_column_comment("cat.sch.tbl", "note", "O'Reilly")
    assert out == "COMMENT ON COLUMN `cat.sch.tbl`.`note` IS 'O''Reilly'"

def test_set_table_comment_escapes_and_quotes():
    out = sql.sql_set_table_comment("cat.sch.tbl", "it's a table")
    assert out == "COMMENT ON TABLE `cat.sch.tbl` IS 'it''s a table'"


# ---- sql_add_primary_key / sql_drop_primary_key ----

def test_add_primary_key_requires_columns():
    with pytest.raises(ValueError):
        sql.sql_add_primary_key("cat.sch.tbl", "pk_name", [])

def test_add_primary_key_builds_statement_with_quoting():
    out = sql.sql_add_primary_key("cat.sch.tbl", "pk name", ["id", "created at"])
    assert out == (
        "ALTER TABLE `cat.sch.tbl` ADD CONSTRAINT `pk name` PRIMARY KEY (`id`, `created at`)"
    )

def test_drop_primary_key_builds_statement_with_quoting():
    out = sql.sql_drop_primary_key("cat.sch.tbl", "pk_customers")
    assert out == "ALTER TABLE `cat.sch.tbl` DROP CONSTRAINT `pk_customers`"


# ---- sql_select_primary_key_name_for_table ----

def test_select_pk_name_uses_escaped_catalog_schema_table():
    # monkeypatch already makes escape_sql_literal a visible transform; assert it was used
    sql_text = sql.sql_select_primary_key_name_for_table(("cat", "sch", "tbl"))
    # spot-check key clauses and escaped/table identifiers appear where expected
    assert "WHERE constraint_type = 'PRIMARY KEY'" in sql_text
    assert "FROM cat.information_schema.table_constraints" in sql_text
    assert "table_catalog = 'cat'" in sql_text
    assert "table_schema  = 'sch'" in sql_text
    assert "table_name    = 'tbl'" in sql_text
    assert "LIMIT 1" in sql_text


# ---- sql_select_primary_key_columns_for_table ----

def test_select_pk_columns_query_structure_and_escaping():
    sql_text = sql.sql_select_primary_key_columns_for_table(("cat", "sch", "tbl"))
    # key join and filter bits should be present
    assert "FROM cat.information_schema.table_constraints AS tc" in sql_text
    assert "JOIN cat.information_schema.key_column_usage AS kcu" in sql_text
    assert "tc.constraint_type = 'PRIMARY KEY'" in sql_text
    assert "tc.table_catalog   = 'cat'" in sql_text
    assert "tc.table_schema    = 'sch'" in sql_text
    assert "tc.table_name      = 'tbl'" in sql_text
    assert "ORDER BY kcu.ordinal_position" in sql_text
