import os
from inspect import signature

from attr import asdict

from footings.model import Footing, model, step
from footings.attributes import (
    define_asset,
    define_parameter,
)
from footings.audit import (
    AuditContainer,
    # AuditStepContainer,
    AuditConfig,
    AuditStepConfig,
)

from footings.test_tools import assert_footings_files_equal


@model(steps=["_step_1", "_step_2", "_step_3"])
class IntegerModel(Footing):
    """Integer model for testing."""

    a = define_parameter(dtype=int, description="A number A")
    b = define_parameter(dtype=int, description="A number B")
    c = define_parameter(dtype=int, description="A number C")
    d = define_parameter(dtype=int, description="A number D")
    ret_1 = define_asset(dtype=int, description="Results a + b")
    ret_2 = define_asset(dtype=int, description="Results c - d")
    ret_3 = define_asset(dtype=int, description="Result total of step_1 and step_2")

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


def test_audit():
    int_model = IntegerModel(a=1, b=1, c=2, d=2)

    # test default config
    test_default = AuditContainer.create(int_model)
    expected_step_1 = {
        "name": "_step_1",
        "method_name": "_step_1",
        "docstring": "Add a and b together.",
        "uses": ["parameter.a", "parameter.b"],
        "impacts": ["asset.ret_1"],
        "output": {"asset.ret_1": 2},
        "metadata": {},
    }
    expected_step_2 = {
        "name": "_step_2",
        "method_name": "_step_2",
        "docstring": "Subtract d from c",
        "uses": ["parameter.c", "parameter.d"],
        "impacts": ["asset.ret_2"],
        "output": {"asset.ret_2": 0},
        "metadata": {},
    }
    expected_step_3 = {
        "name": "_step_3",
        "method_name": "_step_3",
        "docstring": "Add total of steps 1 and 2.",
        "uses": ["asset.ret_1", "asset.ret_2"],
        "impacts": ["asset.ret_3"],
        "output": {"asset.ret_3": 2},
        "metadata": {},
    }

    expected_default = {
        "name": "IntegerModel",
        "docstring": IntegerModel.__doc__,
        "signature": f"IntegerModel{str(signature(int_model.__class__))}",
        "instantiation": {
            "parameter.a": 1,
            "parameter.b": 1,
            "parameter.c": 2,
            "parameter.d": 2,
        },
        "steps": [expected_step_1, expected_step_2, expected_step_3],
        "output": {"ret_1": 2, "ret_2": 0, "ret_3": 2},
        "config": asdict(AuditConfig()),
    }
    assert test_default.as_audit() == expected_default

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
        "signature": f"IntegerModel{str(signature(int_model.__class__))}",
        "instantiation": {
            "parameter.a": 1,
            "parameter.b": 1,
            "parameter.c": 2,
            "parameter.d": 2,
        },
        "steps": [{"name": "_step_1"}, {"name": "_step_2"}, {"name": "_step_3"}],
        "output": {"ret_1": 2, "ret_2": 0, "ret_3": 2},
        "config": asdict(AuditConfig(step_config=step_config)),
    }
    assert test_no_step_detail.as_audit() == expected_no_step_detail


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
