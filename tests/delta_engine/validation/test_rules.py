import pytest
import pyspark.sql.types as T

from src.delta_engine.actions import (
    AlignTable,
    ColumnAdd,
    ColumnNullabilityChange,
    CreateTable,
    PrimaryKeyAdd,
    PrimaryKeyDefinition,
    TablePlan,
)
from src.delta_engine.models import Column, Table
from src.delta_engine.validation.rules import (
    UnsafePlanError,
    InvalidModelError,
    NoAddNotNullColumns,
    CreatePrimaryKeyColumnsNotNull,
    PrimaryKeyAddMustNotMakeColumnsNullable,
    PrimaryKeyNewColumnsMustBeSetNotNull,
    PrimaryKeyExistingColumnsMustBeSetNotNull,
    PrimaryKeyColumnsNotNull,
    DuplicateColumnNames,
    PrimaryKeyMustBeOrderedSequence,
)

# ----------------- Helpers -----------------


def struct(*fields: tuple[str, T.DataType, bool]) -> T.StructType:
    """Build a StructType from (name, dtype, nullable) tuples."""
    return T.StructType([T.StructField(n, dt, nullable=nl) for n, dt, nl in fields])


def make_create(
    with_pk: bool = True,
    fields: tuple[str, T.DataType, bool] | None = None,
) -> CreateTable:
    schema = struct(
        *(fields or (("id", T.IntegerType(), False), ("name", T.StringType(), True)))
    )
    pk = PrimaryKeyDefinition("pk_customers", ("id",)) if with_pk else None
    return CreateTable(
        catalog_name="cat",
        schema_name="sch",
        table_name="tbl",
        schema_struct=schema,
        table_comment="",
        table_properties={},
        column_comments={},
        primary_key=pk,
    )


def make_align(**kwargs) -> AlignTable:
    base = dict(
        catalog_name="cat",
        schema_name="sch",
        table_name="tbl",
        add_columns=tuple(),
        drop_columns=tuple(),
        change_nullability=tuple(),
        set_column_comments=None,
        set_table_comment=None,
        set_table_properties=None,
        drop_primary_key=None,
        add_primary_key=None,
    )
    base.update(kwargs)
    return AlignTable(**base)


# ----------------- NoAddNotNullColumns -----------------


def test_no_add_not_null_columns_allows_nullable_adds():
    plan = TablePlan(
        create_tables=(),
        align_tables=(
            make_align(add_columns=(ColumnAdd("age", T.IntegerType(), True),)),
        ),
    )
    NoAddNotNullColumns().check(plan)  # should not raise


def test_no_add_not_null_columns_rejects_not_null_add():
    plan = TablePlan(
        create_tables=(),
        align_tables=(
            make_align(add_columns=(ColumnAdd("age", T.IntegerType(), False),)),
        ),
    )
    with pytest.raises(UnsafePlanError) as err:
        NoAddNotNullColumns().check(plan)
    assert "ADD COLUMN" in str(err.value)
    assert "`age`" in str(err.value)


# ----------------- CreatePrimaryKeyColumnsNotNull -----------------


def test_create_pk_columns_not_null_passes_when_present_and_not_null():
    plan = TablePlan(create_tables=(make_create(with_pk=True),), align_tables=())
    CreatePrimaryKeyColumnsNotNull().check(plan)  # no raise


def test_create_pk_columns_not_null_fails_when_missing_column():
    ct = make_create(with_pk=True, fields=(("name", T.StringType(), False),))
    plan = TablePlan(create_tables=(ct,), align_tables=())
    with pytest.raises(UnsafePlanError) as err:
        CreatePrimaryKeyColumnsNotNull().check(plan)
    assert "missing" in str(err.value).lower()


def test_create_pk_columns_not_null_fails_when_pk_column_nullable():
    ct = make_create(
        with_pk=True,
        fields=(("id", T.IntegerType(), True), ("name", T.StringType(), True)),
    )
    plan = TablePlan(create_tables=(ct,), align_tables=())
    with pytest.raises(UnsafePlanError) as err:
        CreatePrimaryKeyColumnsNotNull().check(plan)
    assert "must be NOT NULL".lower() in str(err.value).lower()


# ----------------- PrimaryKeyAddMustNotMakeColumnsNullable -----------------


def test_pk_add_must_not_make_columns_nullable_rejects_relaxing_pk_col():
    at = make_align(
        add_primary_key=PrimaryKeyAdd(PrimaryKeyDefinition("pk_t", ("id",))),
        change_nullability=(ColumnNullabilityChange("id", make_nullable=True),),
    )
    plan = TablePlan(create_tables=(), align_tables=(at,))
    with pytest.raises(UnsafePlanError) as err:
        PrimaryKeyAddMustNotMakeColumnsNullable().check(plan)
    assert "cannot be made NULLABLE" in str(err.value)


def test_pk_add_must_not_make_columns_nullable_all_good_when_no_relax():
    at = make_align(
        add_primary_key=PrimaryKeyAdd(PrimaryKeyDefinition("pk_t", ("id",))),
        change_nullability=(ColumnNullabilityChange("id", make_nullable=False),),
    )
    plan = TablePlan(create_tables=(), align_tables=(at,))
    PrimaryKeyAddMustNotMakeColumnsNullable().check(plan)  # no raise


# ----------------- PrimaryKeyNewColumnsMustBeSetNotNull -----------------


def test_pk_new_columns_must_be_set_not_null_requires_tighten_for_new_cols_missing_rule():
    at = make_align(
        add_columns=(ColumnAdd("id", T.IntegerType(), True),),
        add_primary_key=PrimaryKeyAdd(PrimaryKeyDefinition("pk_t", ("id",))),
        change_nullability=(),  # missing tighten
    )
    plan = TablePlan(create_tables=(), align_tables=(at,))
    with pytest.raises(UnsafePlanError) as err:
        PrimaryKeyNewColumnsMustBeSetNotNull().check(plan)
    assert "SET NOT NULL" in str(err.value)


def test_pk_new_columns_must_be_set_not_null_rejects_if_marked_nullable():
    at = make_align(
        add_columns=(ColumnAdd("id", T.IntegerType(), True),),
        add_primary_key=PrimaryKeyAdd(PrimaryKeyDefinition("pk_t", ("id",))),
        change_nullability=(ColumnNullabilityChange("id", make_nullable=True),),
    )
    plan = TablePlan(create_tables=(), align_tables=(at,))
    with pytest.raises(UnsafePlanError):
        PrimaryKeyNewColumnsMustBeSetNotNull().check(plan)


def test_pk_new_columns_must_be_set_not_null_passes_if_tightened():
    at = make_align(
        add_columns=(ColumnAdd("id", T.IntegerType(), True),),
        add_primary_key=PrimaryKeyAdd(PrimaryKeyDefinition("pk_t", ("id",))),
        change_nullability=(ColumnNullabilityChange("id", make_nullable=False),),
    )
    plan = TablePlan(create_tables=(), align_tables=(at,))
    PrimaryKeyNewColumnsMustBeSetNotNull().check(plan)  # no raise


# ----------------- PrimaryKeyExistingColumnsMustBeSetNotNull -----------------


def test_pk_existing_columns_must_be_set_not_null_requires_tighten_for_existing_cols():
    # PK over id + name, but neither is added in this action -> both must be tightened
    at = make_align(
        add_primary_key=PrimaryKeyAdd(PrimaryKeyDefinition("pk_t", ("id", "name"))),
        change_nullability=(ColumnNullabilityChange("id", make_nullable=False),),  # missing name
    )
    plan = TablePlan(create_tables=(), align_tables=(at,))
    with pytest.raises(UnsafePlanError) as err:
        PrimaryKeyExistingColumnsMustBeSetNotNull().check(plan)
    assert "`name`" in str(err.value)


def test_pk_existing_columns_must_be_set_not_null_passes_when_all_existing_cols_tightened():
    at = make_align(
        add_primary_key=PrimaryKeyAdd(PrimaryKeyDefinition("pk_t", ("id", "name"))),
        change_nullability=(
            ColumnNullabilityChange("id", make_nullable=False),
            ColumnNullabilityChange("name", make_nullable=False),
        ),
    )
    plan = TablePlan(create_tables=(), align_tables=(at,))
    PrimaryKeyExistingColumnsMustBeSetNotNull().check(plan)  # no raise


def test_pk_existing_columns_rule_ignores_newly_added_cols():
    # id is added here (so it's "new"); rule should only require tighten on name
    at = make_align(
        add_columns=(ColumnAdd("id", T.IntegerType(), True),),
        add_primary_key=PrimaryKeyAdd(PrimaryKeyDefinition("pk_t", ("id", "name"))),
        change_nullability=(ColumnNullabilityChange("name", make_nullable=False),),
    )
    plan = TablePlan(create_tables=(), align_tables=(at,))
    PrimaryKeyExistingColumnsMustBeSetNotNull().check(plan)  # no raise


# ----------------- Model rules: PrimaryKeyColumnsNotNull -----------------


def test_model_pk_columns_not_null_passes():
    model = Table(
        catalog_name="cat",
        schema_name="sch",
        table_name="tbl",
        columns=[
            Column("id", T.IntegerType(), is_nullable=False),
            Column("name", T.StringType(), is_nullable=True),
        ],
        primary_key=["id"],
    )
    PrimaryKeyColumnsNotNull().check([model])  # no raise


def test_model_pk_columns_not_null_fails_when_missing_column():
    model = Table(
        catalog_name="cat",
        schema_name="sch",
        table_name="tbl",
        columns=[Column("name", T.StringType(), is_nullable=False)],
        primary_key=["id"],
    )
    with pytest.raises(InvalidModelError) as err:
        PrimaryKeyColumnsNotNull().check([model])
    assert "missing" in str(err.value).lower()
    assert "`id`" in str(err.value)


def test_model_pk_columns_not_null_fails_when_nullable():
    model = Table(
        catalog_name="cat",
        schema_name="sch",
        table_name="tbl",
        columns=[Column("id", T.IntegerType(), is_nullable=True)],
        primary_key=["id"],
    )
    with pytest.raises(InvalidModelError) as err:
        PrimaryKeyColumnsNotNull().check([model])
    assert "NOT NULL".lower() in str(err.value).lower()


# ----------------- Model rules: DuplicateColumnNames -----------------


def test_duplicate_column_names_detects_case_insensitive_dupes():
    model = Table(
        catalog_name="c",
        schema_name="s",
        table_name="t",
        columns=[
            Column("Id", T.IntegerType(), is_nullable=False),
            Column("id", T.IntegerType(), is_nullable=False),
            Column("NAME", T.StringType()),
        ],
    )
    with pytest.raises(InvalidModelError) as err:
        DuplicateColumnNames().check([model])
    assert "`id`" in str(err.value).lower()


def test_duplicate_column_names_passes_when_unique():
    model = Table(
        catalog_name="c",
        schema_name="s",
        table_name="t",
        columns=[Column("id", T.IntegerType()), Column("name", T.StringType())],
    )
    DuplicateColumnNames().check([model])  # no raise


# ----------------- Model rules: PrimaryKeyMustBeOrderedSequence -----------------


@pytest.mark.parametrize("bad_pk", [{"id"}, {"id": 1}, "id", 123])
def test_primary_key_must_be_ordered_sequence_rejects_non_sequence(bad_pk):
    model = Table(
        catalog_name="c",
        schema_name="s",
        table_name="t",
        columns=[Column("id", T.IntegerType(), is_nullable=False)],
        primary_key=bad_pk,  # type: ignore[arg-type]
    )
    with pytest.raises(InvalidModelError):
        PrimaryKeyMustBeOrderedSequence().check([model])


@pytest.mark.parametrize("good_pk", [["id", "name"], ("id", "name")])
def test_primary_key_must_be_ordered_sequence_accepts_list_or_tuple(good_pk):
    model = Table(
        catalog_name="c",
        schema_name="s",
        table_name="t",
        columns=[
            Column("id", T.IntegerType(), is_nullable=False),
            Column("name", T.StringType(), is_nullable=False),
        ],
        primary_key=list(good_pk) if isinstance(good_pk, list) else good_pk,  # keep order
    )
    PrimaryKeyMustBeOrderedSequence().check([model])  # no raise
