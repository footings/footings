import os
import datetime

from attr import attrs, attrib, evolve
import pandas as pd

from footings.model import model, step, def_parameter, def_return
from footings.jigs import WrappedModel
from footings.io.to_xlsx import FootingsXlsxWb
from footings.testing import assert_footings_files_equal


def test_footings_xlsx_wb(tmp_path):

    wb = FootingsXlsxWb.create()

    # test builtins
    wb.create_sheet("test-builtins", start_row=2, start_col=2)
    wb.write_obj("test-builtins", "test-bool", add_cols=1)
    wb.write_obj("test-builtins", True, add_rows=1, add_cols=-1)
    wb.write_obj("test-builtins", "test-str", add_cols=1)
    wb.write_obj("test-builtins", "string", add_rows=1, add_cols=-1)
    wb.write_obj("test-builtins", "test-int", add_cols=1)
    wb.write_obj("test-builtins", 1, add_rows=1, add_cols=-1)
    wb.write_obj("test-builtins", "test-float", add_cols=1)
    wb.write_obj("test-builtins", 1.23456789, add_rows=1, add_cols=-1)
    wb.write_obj("test-builtins", "test-date", add_cols=1)
    wb.write_obj("test-builtins", datetime.date(2018, 12, 31), add_rows=1, add_cols=-1)
    wb.write_obj("test-builtins", "test-datetime", add_cols=1)
    wb.write_obj(
        "test-builtins",
        datetime.datetime(2018, 12, 31, 12, 0, 0, 0),
        add_rows=1,
        add_cols=-1,
    )

    # test pd.Series
    wb.create_sheet("test-series", start_row=2, start_col=2)
    wb.write_obj("test-series", pd.Series([1, 2, 3], name="numbers"), add_rows=2)
    wb.write_obj("test-series", pd.Series(["a", "b", "c"], name="letters"), add_rows=2)

    # test pd.DataFrame
    wb.create_sheet("test-dataframe", start_row=2, start_col=2)
    df = pd.DataFrame({"numbers": [1, 2], "letters": ["a", "b"]}, None)
    wb.write_obj("test-dataframe", df)

    # test mapping
    wb.create_sheet("test-mapping", start_row=2, start_col=2)
    wb.write_obj("test-mapping", {"key": "value"}, add_rows=2)
    wb.write_obj("test-mapping", {"key": [1, 2, 3, 4]}, add_rows=2)
    wb.write_obj("test-mapping", {"key": df}, add_rows=2)
    wb.write_obj("test-mapping", {("tuple", "key"): 1}, add_rows=2)

    # test iterable
    wb.create_sheet("test-iterable", start_row=2, start_col=2)
    wb.write_obj("test-iterable", [1, 2, 3], add_rows=2)
    wb.write_obj("test-iterable", [[1, 2, 3], [1, 2, 3]], add_rows=2)
    wb.write_obj("test-iterable", [df, df], add_rows=2)

    # test custom
    @attrs
    class CustomOutput:
        """Custom Output"""

        a: int = attrib()
        b: int = attrib()

        def to_audit_xlsx(self):
            return {"a": self.a, "b": self.b}

    custom1 = CustomOutput(1, 2)
    custom2 = CustomOutput(2, 4)

    wb.create_sheet("test-custom", start_row=2, start_col=2)
    wb.write_obj("test-custom", custom1, add_rows=2)
    wb.write_obj("test-custom", custom2, add_rows=2)

    # test callable
    def test_func(a, b):
        return a, b

    wb.create_sheet("test-function", start_row=2, start_col=2)
    wb.write_obj("test-function", test_func)

    # test Error
    @model(steps=["_add_a_b"])
    class Model1:
        k1 = def_parameter()
        k2 = def_parameter()
        a = def_parameter()
        b = def_parameter()
        r = def_return()

        @step(uses=["a", "b"], impacts=["r"])
        def _add_a_b(self):
            self.r = self.a + self.b

    model1 = WrappedModel(Model1, iterator_keys=("k1",), pass_iterator_keys=("k1",))
    output = model1(k1="1", a=1, b=2)
    output = evolve(output, error_stacktrace="[]")
    wb.create_sheet("test-error-catch", start_row=2, start_col=2)
    wb.write_obj("test-error-catch", output)

    test_wb = os.path.join(tmp_path, "test-footings-wb.xlsx")
    wb.save(test_wb)

    expected_wb = os.path.join("tests", "io", "data", "expected-footings-wb.xlsx")

    assert_footings_files_equal(test_wb, expected_wb)
