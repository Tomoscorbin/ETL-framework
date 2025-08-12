import re

from src.delta_engine.constraints.naming import (
    MAX_IDENTIFIER_LEN,
    _short_hash,
    _truncate_with_hash,
    build_primary_key_name,
)


def test_build_pk_name_format_no_truncation():
    name = build_primary_key_name("cat", "sch", "tbl", ["id", "name"])
    assert name == "pk_cat_sch_tbl__id_name"


def test_build_pk_name_preserves_column_order():
    a_then_b = build_primary_key_name("c", "s", "t", ["a", "b"])
    b_then_a = build_primary_key_name("c", "s", "t", ["b", "a"])
    assert a_then_b != b_then_a
    assert a_then_b.endswith("__a_b")
    assert b_then_a.endswith("__b_a")


def test_build_pk_name_truncates_with_hash_suffix_when_long():
    catalog = "c" * 80
    schema = "s" * 80
    table = "t" * 80
    cols = ["col1", "col2", "col3", "col4", "col5"]
    base = f"pk_{catalog}_{schema}_{table}__{'_'.join(cols)}"

    name = build_primary_key_name(catalog, schema, table, cols)

    # Should be bounded and end with _<8-hex>
    assert len(name) <= MAX_IDENTIFIER_LEN
    h = _short_hash(base)
    assert re.fullmatch(rf".*_{h}", name) is not None

    # Check the prefix length math matches the implementation
    keep = MAX_IDENTIFIER_LEN - 1 - len(h)  # space for '_' + hash
    assert name == f"{base[:keep]}_{h}"


def test_truncate_with_hash_is_noop_when_short():
    s = "abc"
    assert _truncate_with_hash(s, max_len=10) == s


def test_truncate_with_hash_uses_hash_only_when_tiny_limit():
    s = "something-very-long"
    h = _short_hash(s)

    # max_len <= len(hash) -> trimmed hash, no underscore
    out = _truncate_with_hash(s, max_len=5)
    assert out == h[:5]
    assert len(out) == 5

    out8 = _truncate_with_hash(s, max_len=8)
    assert out8 == h  # exactly the hash, length 8


def test_truncate_with_hash_without_space_for_sep_keeps_no_sep():
    s = "abcdefghijklmnopqrstuvwxyz"
    h = _short_hash(s)

    # max_len = 9 -> keep=0, returns base[:1] + hash (no underscore)
    out = _truncate_with_hash(s, max_len=9)
    assert len(out) == 9
    assert out.endswith(h)
    assert out[: -len(h)] == s[:1]  # one char prefix, then hash


def test_short_hash_is_deterministic_and_hex():
    a = _short_hash("x", "y")
    b = _short_hash("x", "y")
    c = _short_hash("x", "z")
    assert a == b
    assert a != c
    assert re.fullmatch(r"[0-9a-f]{8}", a) is not None
