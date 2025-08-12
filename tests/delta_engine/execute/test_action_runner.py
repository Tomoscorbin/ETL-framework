from src.delta_engine.actions import TablePlan
from src.delta_engine.execute.action_runner import ActionRunner

# ---------- helpers ----------


class RecordingExecutor:
    """Minimal fake executor that records apply() calls."""

    def __init__(self, call_log, label):
        self.call_log = call_log
        self.label = label

    def apply(self, action):
        self.call_log.append((self.label, action))


class StubCreateExec:
    def __init__(self, spark):
        self.spark = spark
        self.calls = []

    def apply(self, action):
        self.calls.append(action)


class StubAlignExec:
    def __init__(self, spark):
        self.spark = spark
        self.calls = []

    def apply(self, action):
        self.calls.append(action)


class FakeSpark:
    pass


# ---------- tests ----------


def test_initializes_executors_with_same_spark(monkeypatch):
    """Constructor should pass the same SparkSession to both executors."""
    created = {}

    def make_create_exec(spark):
        ce = StubCreateExec(spark)
        created["create"] = ce
        return ce

    def make_align_exec(spark):
        ae = StubAlignExec(spark)
        created["align"] = ae
        return ae

    # Patch the executor classes in the module under test
    import src.delta_engine.execute.action_runner as mod

    monkeypatch.setattr(mod, "CreateExecutor", make_create_exec)
    monkeypatch.setattr(mod, "AlignExecutor", make_align_exec)

    spark = FakeSpark()
    runner = ActionRunner(spark)

    assert created["create"].spark is spark
    assert created["align"].spark is spark
    # and runner holds those instances
    assert runner._create_executor is created["create"]
    assert runner._align_executor is created["align"]


def test_apply_noop_when_plan_empty():
    spark = FakeSpark()
    runner = ActionRunner(spark)

    # Replace executors with fakes that would record or blow up if called
    calls = []
    runner._create_executor = RecordingExecutor(calls, "create")
    runner._align_executor = RecordingExecutor(calls, "align")

    empty = TablePlan(create_tables=(), align_tables=())
    runner.apply(empty)

    assert calls == []  # nothing called


def test_apply_orders_creates_before_aligns_and_preserves_order():
    spark = FakeSpark()
    runner = ActionRunner(spark)

    calls = []
    runner._create_executor = RecordingExecutor(calls, "create")
    runner._align_executor = RecordingExecutor(calls, "align")

    # Sentinels are fine; ActionRunner doesn't inspect action attributes
    plan = TablePlan(
        create_tables=("C1", "C2"),
        align_tables=("A1", "A2"),
    )

    runner.apply(plan)

    assert calls == [
        ("create", "C1"),
        ("create", "C2"),
        ("align", "A1"),
        ("align", "A2"),
    ]


def test_apply_handles_only_creates_or_only_aligns():
    spark = FakeSpark()
    runner = ActionRunner(spark)

    calls = []
    runner._create_executor = RecordingExecutor(calls, "create")
    runner._align_executor = RecordingExecutor(calls, "align")

    runner.apply(TablePlan(create_tables=("C1",), align_tables=()))
    runner.apply(TablePlan(create_tables=(), align_tables=("A1",)))

    assert calls == [("create", "C1"), ("align", "A1")]
