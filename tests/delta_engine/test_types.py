import pytest
from dataclasses import dataclass
from typing import get_origin, get_args, Protocol

from src.delta_engine.types import ThreePartTableName, HasTableIdentity
from src.delta_engine.utils import qualify_table_name


def test_three_part_table_name_is_tuple_of_three_strings():
    # The alias should be parameterized tuple[str, str, str]
    origin = get_origin(ThreePartTableName)
    args = get_args(ThreePartTableName)
    assert origin is tuple
    assert args == (str, str, str)

    # And a typical value should be a 3-tuple of strings
    example: ThreePartTableName = ("cat", "sch", "tbl")  # type: ignore[assignment]
    assert isinstance(example, tuple)
    assert len(example) == 3
    assert all(isinstance(x, str) for x in example)


def test_has_table_identity_is_a_protocol_but_not_runtime_checkable():
    # Sanity: it's a Protocol subclass
    assert issubclass(HasTableIdentity, Protocol)

    # Not decorated with @runtime_checkable, so isinstance/issubclass should raise TypeError
    @dataclass(frozen=True)
    class Fake:
        catalog_name: str
        schema_name: str
        table_name: str

    with pytest.raises(TypeError):
        isinstance(Fake("c", "s", "t"), HasTableIdentity)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        issubclass(Fake, HasTableIdentity)  # type: ignore[type-arg]


def test_qualify_table_name_accepts_protocol_like_object():
    # Anything with the required attributes should work at runtime
    @dataclass(frozen=True)
    class Fake:
        catalog_name: str
        schema_name: str
        table_name: str

    obj = Fake("cat", "sch", "tbl")
    assert qualify_table_name(obj) == "cat.sch.tbl"
