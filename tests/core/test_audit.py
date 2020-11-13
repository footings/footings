import os

from footings.core.model import Footing, model, step
from footings.core.attributes import (
    define_asset,
    define_parameter,
)
from footings.test_tools import assert_footings_audit_xlsx_equal


@model(steps=["_step_1", "_step_2", "_step_3"])
class IntegerModel(Footing):
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

    def _return(self):
        """Return override

        Returns
        -------
        ret_3
        """
        return self.ret_3


def test_audit_xlsx(tmp_path):

    test_integer_out = os.path.join(tmp_path, "test-integers.xlsx")
    expected_integer_out = os.path.join("tests", "core", "data", "expected-integers.xlsx")
    loaded_integer_model = IntegerModel(a=1, b=1, c=2, d=2)
    loaded_integer_model.audit(file=test_integer_out)

    assert_footings_audit_xlsx_equal(test_integer_out, expected_integer_out)

    # test_pandas_out = os.path.join(tmp_path, "test-pandas.xlsx")
    # expected_pandas_out = os.path.join("tests", "core", "data", "expected-pandas.xlsx")
    # pandas_model = build_model("PandasModel", steps=STEPS_USING_PANDAS)
    # loaded_pandas_model = pandas_model(n=5, add=1, subtract=1)
    # loaded_pandas_model.audit(file=test_pandas_out)


#
# assert_footings_audit_xlsx_equal(test_pandas_out, expected_pandas_out)
