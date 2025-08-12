import pyspark.sql.types as T
import pytest

from src.delta_engine.orchestrate.orchestrator import Orchestrator
from src.delta_engine.models import Table, Column
from src.delta_engine.state.states import CatalogState
from src.delta_engine.actions import TablePlan
import src.delta_engine.orchestrate.orchestrator as orch_mod  # for monkeypatching LOGGER


# ---------- fakes ----------

class FakeSpark:
    pass


class FakeReader:
    def __init__(self):
        self.calls = []
        self.result = CatalogState(tables={})

    def snapshot(self, desired_tables):
        self.calls.append(("snapshot", tuple(desired_tables)))
        return self.result


class FakePlanner:
    def __init__(self, create=(), align=()):
        self.calls = []
        self._plan = TablePlan(create_tables=tuple(create), align_tables=tuple(align))

    def plan(self, desired_tables, catalog_state):
        self.calls.append(("plan", tuple(desired_tables), catalog_state))
        return self._plan



class FakeValidator:
    def __init__(self, model_exc=None, plan_exc=None):
        self.calls = []
        self.model_exc = model_exc
        self.plan_exc = plan_exc

    def validate_models(self, models):
        self.calls.append(("validate_models", tuple(models)))
        if self.model_exc:
            raise self.model_exc

    def validate_plan(self, plan):
        self.calls.append(("validate_plan", plan))
        if self.plan_exc:
            raise self.plan_exc


class FakeRunner:
    def __init__(self):
        self.calls = []

    def apply(self, plan):
        self.calls.append(("apply", plan))


class FakeLogger:
    def __init__(self):
        self.messages = []

    def info(self, msg, *args):
        # support %-format args used in logger calls
        self.messages.append(msg % args if args else msg)


def make_table():
    return Table(
        catalog_name="cat",
        schema_name="sch",
        table_name="tbl",
        columns=[Column("id", T.IntegerType(), is_nullable=False)],
        comment="",
        table_properties={},
        primary_key=None,
    )


# ---------- tests ----------

def test_default_components_wire_spark(monkeypatch):
    # use real classes but ensure LOGGER doesn't blow up
    monkeypatch.setattr(orch_mod, "LOGGER", FakeLogger(), raising=True)
    spark = FakeSpark()
    o = Orchestrator(spark)

    # Reader and Runner should hold the same spark instance
    assert o.reader.spark is spark
    assert o.runner.spark is spark
    # Planner/Validator exist
    assert o.table_planner is not None
    assert o.validator is not None


def test_injected_components_are_used(monkeypatch):
    monkeypatch.setattr(orch_mod, "LOGGER", FakeLogger(), raising=True)
    spark = FakeSpark()
    reader = FakeReader()
    planner = FakePlanner()
    validator = FakeValidator()
    runner = FakeRunner()

    o = Orchestrator(spark, reader=reader, planner=planner, validator=validator, runner=runner)
    assert o.reader is reader
    assert o.table_planner is planner
    assert o.validator is validator
    assert o.runner is runner


def test_sync_tables_happy_path_calls_in_order_and_logs(monkeypatch):
    log = FakeLogger()
    monkeypatch.setattr(orch_mod, "LOGGER", log, raising=True)

    reader = FakeReader()
    planner = FakePlanner(create=("C1",), align=("A1", "A2"))
    validator = FakeValidator()
    runner = FakeRunner()

    spark = FakeSpark()
    o = Orchestrator(spark, reader=reader, planner=planner, validator=validator, runner=runner)

    desired = [make_table()]
    o.sync_tables(desired)

    # call order
    assert reader.calls == [("snapshot", tuple(desired))]
    assert planner.calls and planner.calls[0][0] == "plan"
    # validator called with models then plan
    assert validator.calls[0][0] == "validate_models"
    assert validator.calls[1][0] == "validate_plan"
    # runner executed
    assert runner.calls and runner.calls[0][0] == "apply"

    # logs
    assert any("Starting orchestration for 1 table" in m for m in log.messages)
    assert any("Plan generated" in m and "create=1, align=2" in m for m in log.messages)
    assert any("Orchestration completed." in m for m in log.messages)


def test_sync_tables_fail_fast_on_model_validation(monkeypatch):
    log = FakeLogger()
    monkeypatch.setattr(orch_mod, "LOGGER", log, raising=True)

    reader = FakeReader()
    planner = FakePlanner(create=(), align=())
    validator = FakeValidator(model_exc=RuntimeError("model boom"))
    runner = FakeRunner()

    o = Orchestrator(FakeSpark(), reader=reader, planner=planner, validator=validator, runner=runner)

    with pytest.raises(RuntimeError):
        o.sync_tables([make_table()])

    # runner not called; plan validation not called
    assert not runner.calls
    assert [c[0] for c in validator.calls] == ["validate_models"]


def test_sync_tables_fail_fast_on_plan_validation(monkeypatch):
    log = FakeLogger()
    monkeypatch.setattr(orch_mod, "LOGGER", log, raising=True)

    reader = FakeReader()
    planner = FakePlanner(create=("C",), align=())
    validator = FakeValidator(plan_exc=RuntimeError("plan boom"))
    runner = FakeRunner()

    o = Orchestrator(FakeSpark(), reader=reader, planner=planner, validator=validator, runner=runner)

    with pytest.raises(RuntimeError):
        o.sync_tables([make_table()])

    # runner not called; both validations were attempted
    assert not runner.calls
    assert [c[0] for c in validator.calls] == ["validate_models", "validate_plan"]


def test_snapshot_compile_validate_execute_helpers_are_thin_wrappers(monkeypatch):
    """Smoke-test the private helpers map straight to the collaborators."""
    # no logger patch needed
    reader = FakeReader()
    planner = FakePlanner(create=("X",), align=("Y",))
    validator = FakeValidator()
    runner = FakeRunner()

    o = Orchestrator(FakeSpark(), reader=reader, planner=planner, validator=validator, runner=runner)

    desired = [make_table()]
    snap = o._snapshot(desired)
    assert isinstance(snap, CatalogState)

    plan = o._compile(desired, snap)
    assert isinstance(plan, TablePlan)
    assert len(plan.create_tables) == 1 and len(plan.align_tables) == 1

    o._validate(desired, plan)  # should not raise
    o._execute(plan)
    assert runner.calls and runner.calls[0] == ("apply", plan)
