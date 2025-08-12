import pytest

from src.delta_engine.validation.validator import Validator


# --------- Stubs ---------

class StubModelViolation(Exception):
    pass


class StubPlanViolation(Exception):
    pass


class StubModelRule:
    def __init__(self, name, to_raise=None, call_log=None):
        self.name = name
        self.to_raise = to_raise
        self.call_log = call_log

    def check(self, models):
        if self.call_log is not None:
            self.call_log.append(("model", self.name, models))
        if self.to_raise:
            raise self.to_raise


class StubPlanRule:
    def __init__(self, name, to_raise=None, call_log=None):
        self.name = name
        self.to_raise = to_raise
        self.call_log = call_log

    def check(self, plan):
        if self.call_log is not None:
            self.call_log.append(("plan", self.name, plan))
        if self.to_raise:
            raise self.to_raise


# --------- Defaults ---------

def test_defaults_are_used_and_are_tuples():
    v = Validator()
    # identity and type checks on defaults (don’t execute them)
    assert isinstance(v.model_rules, tuple)
    assert isinstance(v.plan_rules, tuple)
    assert v.model_rules is Validator.DEFAULT_MODEL_RULES
    assert v.plan_rules is Validator.DEFAULT_PLAN_RULES
    # preserve ordering of defaults
    assert list(v.model_rules) == list(Validator.DEFAULT_MODEL_RULES)
    assert list(v.plan_rules) == list(Validator.DEFAULT_PLAN_RULES)


# --------- Overrides ---------

def test_overrides_are_coerced_to_tuples_and_order_preserved():
    log = []
    r1 = StubModelRule("m1", call_log=log)
    r2 = StubModelRule("m2", call_log=log)
    p1 = StubPlanRule("p1", call_log=log)
    p2 = StubPlanRule("p2", call_log=log)

    v = Validator(model_rules=[r1, r2], plan_rules=[p1, p2])
    assert isinstance(v.model_rules, tuple)
    assert isinstance(v.plan_rules, tuple)
    assert v.model_rules == (r1, r2)
    assert v.plan_rules == (p1, p2)


def test_empty_override_lists_mean_noops():
    v = Validator(model_rules=[], plan_rules=[])
    # Should not raise
    v.validate_models(models=[object()])
    v.validate_plan(plan=object())


# --------- validate_models behavior ---------

def test_validate_models_calls_each_rule_in_order_and_passes_models_through():
    log = []
    m1 = StubModelRule("m1", call_log=log)
    m2 = StubModelRule("m2", call_log=log)
    models = ["modelA", "modelB"]

    v = Validator(model_rules=[m1, m2], plan_rules=[])
    v.validate_models(models)

    assert log == [
        ("model", "m1", models),
        ("model", "m2", models),
    ]


def test_validate_models_is_fail_fast_on_first_violation():
    log = []
    m1 = StubModelRule("m1", to_raise=StubModelViolation("boom"), call_log=log)
    m2 = StubModelRule("m2", call_log=log)

    v = Validator(model_rules=[m1, m2], plan_rules=[])
    with pytest.raises(StubModelViolation):
        v.validate_models(models=["irrelevant"])

    # second rule must not run
    assert log == [("model", "m1", ["irrelevant"])]


# --------- validate_plan behavior ---------

def test_validate_plan_calls_each_rule_in_order_and_passes_plan_through():
    log = []
    p1 = StubPlanRule("p1", call_log=log)
    p2 = StubPlanRule("p2", call_log=log)
    plan = {"plan": "payload"}

    v = Validator(model_rules=[], plan_rules=[p1, p2])
    v.validate_plan(plan)

    assert log == [
        ("plan", "p1", plan),
        ("plan", "p2", plan),
    ]


def test_validate_plan_is_fail_fast_on_first_violation():
    log = []
    p1 = StubPlanRule("p1", to_raise=StubPlanViolation("nope"), call_log=log)
    p2 = StubPlanRule("p2", call_log=log)

    v = Validator(model_rules=[], plan_rules=[p1, p2])
    with pytest.raises(StubPlanViolation):
        v.validate_plan(plan={"x": 1})

    assert log == [("plan", "p1", {"x": 1})]
