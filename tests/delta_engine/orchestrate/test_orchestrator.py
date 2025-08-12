from __future__ import annotations

import pyspark.sql.types as T
import pytest

import src.delta_engine.orchestrate.orchestrator as orch_mod  # for monkeypatching LOGGER
from src.delta_engine.actions import TablePlan
from src.delta_engine.models import Column, Table
from src.delta_engine.orchestrate.orchestrator import Orchestrator
from src.delta_engine.state.states import CatalogState

# ---------- fakes ----------


class FakeSpark:
    pass


class FakeReader:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Table, ...]]] = []
        self.result = CatalogState(tables={})

    def snapshot(self, desired_tables: list[Table] | tuple[Table, ...]) -> CatalogState:
        self.calls.append(("snapshot", tuple(desired_tables)))
        return self.result


class FakePlanner:
    def __init__(self, create: tuple = (), align: tuple = ()) -> None:
        self.calls: list[tuple[str, tuple[Table, ...], CatalogState]] = []
        self._plan = TablePlan(create_tables=tuple(create), align_tables=tuple(align))

    def plan(
        self, desired_tables: list[Table] | tuple[Table, ...], catalog_state: CatalogState
    ) -> TablePlan:
        self.calls.append(("plan", tuple(desired_tables), catalog_state))
        return self._plan


class FakeValidator:
    def __init__(
        self, model_exc: Exception | None = None, plan_exc: Exception | None = None
    ) -> None:
        self.calls: list[tuple[str, object]] = []
        self.model_exc = model_exc
        self.plan_exc = plan_exc

    def validate_models(self, models: list[Table] | tuple[Table, ...]) -> None:
        self.calls.append(("validate_models", tuple(models)))
        if self.model_exc:
            raise self.model_exc

    def validate_plan(self, plan: TablePlan) -> None:
        self.calls.append(("validate_plan", plan))
        if self.plan_exc:
            raise self.plan_exc


class FakeRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, TablePlan]] = []

    def apply(self, plan: TablePlan) -> None:
        self.calls.append(("apply", plan))


class FakeLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, msg: str, *args) -> None:
        # match logging API: %-style formatting
        self.messages.append(msg % args if args else msg)


# ---------- helpers ----------


def make_table() -> Table:
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


def test_default_components_wire_spark(monkeypatch) -> None:
    monkeypatch.setattr(orch_mod, "LOGGER", FakeLogger(), raising=True)
    spark = FakeSpark()
    orchestrator = Orchestrator(spark)

    assert orchestrator.reader.spark is spark
    assert orchestrator.runner.spark is spark
    assert orchestrator.table_planner is not None
    assert orchestrator.validator is not None


def test_injected_components_are_used(monkeypatch) -> None:
    monkeypatch.setattr(orch_mod, "LOGGER", FakeLogger(), raising=True)
    spark = FakeSpark()
    reader = FakeReader()
    planner = FakePlanner()
    validator = FakeValidator()
    runner = FakeRunner()

    orchestrator = Orchestrator(
        spark, reader=reader, planner=planner, validator=validator, runner=runner
    )

    assert orchestrator.reader is reader
    assert orchestrator.table_planner is planner
    assert orchestrator.validator is validator
    assert orchestrator.runner is runner


def test_sync_tables_happy_path_calls_in_order_and_logs(monkeypatch) -> None:
    log = FakeLogger()
    monkeypatch.setattr(orch_mod, "LOGGER", log, raising=True)

    reader = FakeReader()
    planner = FakePlanner(create=("C1",), align=("A1", "A2"))
    validator = FakeValidator()
    runner = FakeRunner()

    orchestrator = Orchestrator(
        FakeSpark(), reader=reader, planner=planner, validator=validator, runner=runner
    )

    desired = [make_table()]
    orchestrator.sync_tables(desired)

    assert reader.calls == [("snapshot", tuple(desired))]
    assert planner.calls  # not empty
    assert planner.calls[0][0] == "plan"
    assert [c[0] for c in validator.calls] == ["validate_models", "validate_plan"]
    assert runner.calls
    assert runner.calls[0][0] == "apply"

    assert any("Starting orchestration for 1 table" in m for m in log.messages)
    assert any("Plan generated" in m and "create=1, align=2" in m for m in log.messages)
    assert any("Orchestration completed." in m for m in log.messages)


def test_sync_tables_fail_fast_on_model_validation(monkeypatch) -> None:
    log = FakeLogger()
    monkeypatch.setattr(orch_mod, "LOGGER", log, raising=True)

    reader = FakeReader()
    planner = FakePlanner()
    validator = FakeValidator(model_exc=RuntimeError("model boom"))
    runner = FakeRunner()

    orchestrator = Orchestrator(
        FakeSpark(), reader=reader, planner=planner, validator=validator, runner=runner
    )

    with pytest.raises(RuntimeError, match="model boom"):
        orchestrator.sync_tables([make_table()])

    assert [c[0] for c in validator.calls] == ["validate_models"]
    assert not runner.calls  # nothing executed


def test_sync_tables_fail_fast_on_plan_validation(monkeypatch) -> None:
    log = FakeLogger()
    monkeypatch.setattr(orch_mod, "LOGGER", log, raising=True)

    reader = FakeReader()
    planner = FakePlanner(create=("C",), align=())
    validator = FakeValidator(plan_exc=RuntimeError("plan boom"))
    runner = FakeRunner()

    orchestrator = Orchestrator(
        FakeSpark(), reader=reader, planner=planner, validator=validator, runner=runner
    )

    with pytest.raises(RuntimeError, match="plan boom"):
        orchestrator.sync_tables([make_table()])

    assert [c[0] for c in validator.calls] == ["validate_models", "validate_plan"]
    assert not runner.calls


def test_private_helpers_are_thin_wrappers() -> None:
    reader = FakeReader()
    planner = FakePlanner(create=("X",), align=("Y",))
    validator = FakeValidator()
    runner = FakeRunner()

    orchestrator = Orchestrator(
        FakeSpark(), reader=reader, planner=planner, validator=validator, runner=runner
    )

    desired = [make_table()]
    snap = orchestrator._snapshot(desired)
    assert isinstance(snap, CatalogState)

    plan = orchestrator._compile(desired, snap)
    assert isinstance(plan, TablePlan)
    assert len(plan.create_tables) == 1
    assert len(plan.align_tables) == 1

    orchestrator._validate(desired, plan)  # should not raise
    orchestrator._execute(plan)

    assert runner.calls
    assert runner.calls[0] == ("apply", plan)
