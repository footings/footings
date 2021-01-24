import os

from footings.model import (
    model,
    step,
    def_parameter,
    def_intermediate,
    def_return,
)
from footings.audit import (
    AuditContainer,
    AuditStepContainer,
    AuditConfig,
    AuditStepConfig,
)

from footings.testing import assert_footings_files_equal


@model(steps=["_step_1", "_step_2", "_step_3"])
class IntegerModel:
    """Integer model for testing."""

    a = def_parameter(dtype=int, description="A number A")
    b = def_parameter(dtype=int, description="A number B")
    c = def_parameter(dtype=int, description="A number C")
    d = def_parameter(dtype=int, description="A number D")
    ret_1 = def_intermediate(dtype=int, description="Results a + b")
    ret_2 = def_intermediate(dtype=int, description="Results c - d")
    ret_3 = def_return(dtype=int, description="Result total of step_1 and step_2")

    @step(uses=["a", "b"], impacts=["ret_1"])
    def _step_1(self):
        """Add a and b together."""
        self.ret_1 = self.a + self.b

    @step(uses=["c", "d"], impacts=["ret_2"])
    def _step_2(self):
        """Subtract d from c"""
        self.ret_2 = self.c - self.d

    @step(uses=["ret_1", "ret_2"], impacts=["ret_3"])
    def _step_3(self):
        """Add total of steps 1 and 2."""
        self.ret_3 = self.ret_1 + self.ret_2


expected_step_1 = {
    "name": "_step_1",
    "method_name": "_step_1",
    "docstring": "Add a and b together.",
    "uses": ("parameter.a", "parameter.b"),
    "impacts": ("intermediate.ret_1",),
    "output": {"intermediate.ret_1": 2},
    "metadata": {},
    "config": AuditStepConfig(),
}

expected_step_2 = {
    "name": "_step_2",
    "method_name": "_step_2",
    "docstring": "Subtract d from c",
    "uses": ("parameter.c", "parameter.d"),
    "impacts": ("intermediate.ret_2",),
    "output": {"intermediate.ret_2": 0},
    "metadata": {},
    "config": AuditStepConfig(),
}

expected_step_3 = {
    "name": "_step_3",
    "method_name": "_step_3",
    "docstring": "Add total of steps 1 and 2.",
    "uses": ("intermediate.ret_1", "intermediate.ret_2"),
    "impacts": ("return.ret_3",),
    "output": {"return.ret_3": 2},
    "metadata": {},
    "config": AuditStepConfig(),
}


def test_audit():
    int_model = IntegerModel(a=1, b=1, c=2, d=2)
    test_default = AuditContainer.create(int_model)
    expected_default = AuditContainer(
        name="IntegerModel",
        docstring=IntegerModel.__doc__,
        signature="IntegerModel(*, a: int, b: int, c: int, d: int)",
        instantiation={
            "parameter.a": 1,
            "parameter.b": 1,
            "parameter.c": 2,
            "parameter.d": 2,
        },
        steps={
            "_step_1": AuditStepContainer(**expected_step_1),
            "_step_2": AuditStepContainer(**expected_step_2),
            "_step_3": AuditStepContainer(**expected_step_3),
        },
        output={"ret_3": 2},
        config=AuditConfig(),
    )
    assert test_default == expected_default

    # test exclude all step detail
    step_config = AuditStepConfig(
        show_method_name=False,
        show_docstring=False,
        show_uses=False,
        show_impacts=False,
        show_output=False,
        show_metadata=False,
    )
    test_no_step_detail = AuditContainer.create(
        int_model, config=AuditConfig(step_config=step_config)
    )
    expected_no_step_detail = {
        "name": "IntegerModel",
        "docstring": IntegerModel.__doc__,
        "signature": "IntegerModel(*, a: int, b: int, c: int, d: int)",
        "instantiation": {
            "parameter.a": 1,
            "parameter.b": 1,
            "parameter.c": 2,
            "parameter.d": 2,
        },
        "steps": {
            "_step_1": AuditStepContainer(name="_step_1", config=step_config),
            "_step_2": AuditStepContainer(name="_step_2", config=step_config),
            "_step_3": AuditStepContainer(name="_step_3", config=step_config),
        },
        "output": {"ret_3": 2},
        "config": AuditConfig(step_config=step_config),
    }
    assert test_no_step_detail == AuditContainer(**expected_no_step_detail)


def test_audit_python():
    expected = AuditContainer(
        name="IntegerModel",
        docstring=IntegerModel.__doc__,
        signature="IntegerModel(*, a: int, b: int, c: int, d: int)",
        instantiation={
            "parameter.a": 1,
            "parameter.b": 1,
            "parameter.c": 2,
            "parameter.d": 2,
        },
        steps={
            "_step_1": AuditStepContainer(**expected_step_1),
            "_step_2": AuditStepContainer(**expected_step_2),
            "_step_3": AuditStepContainer(**expected_step_3),
        },
        output={"ret_3": 2},
        config=AuditConfig(),
    )
    assert IntegerModel(a=1, b=1, c=2, d=2).audit() == expected


def test_audit_xlsx(tmp_path):
    test_integer_out = os.path.join(tmp_path, "test-integers.xlsx")
    expected_integer_out = os.path.join("tests", "data", "expected-integers.xlsx")
    loaded_integer_model = IntegerModel(a=1, b=1, c=2, d=2)
    loaded_integer_model.audit(file=test_integer_out)
    assert_footings_files_equal(test_integer_out, expected_integer_out)


def test_audit_json(tmp_path):
    test_integer_out = os.path.join(tmp_path, "test-integers.json")
    expected_integer_out = os.path.join("tests", "data", "expected-integers.json")
    loaded_integer_model = IntegerModel(a=1, b=1, c=2, d=2)
    loaded_integer_model.audit(file=test_integer_out)
    assert_footings_files_equal(test_integer_out, expected_integer_out)
