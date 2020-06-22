"""Test for audit.py"""

import os

from footings import create_model

from .shared import STEPS_USING_INTEGERS, STEPS_USING_PANDAS
from .xlsx_helpers import compare_xlsx_files

# def test_audit_json(tmp_path):
#     integer_model = create_model("IntegerModel", steps=steps_using_integers())
#     loaded_integer_model = integer_model(a=1, b=1, c=1)
#     loaded_integer_model.audit(output_type="json", file="test-integers.json", indent=2)
#     pandas_model = create_model("PandasModel", steps=steps_using_pandas())
#     loaded_pandas_model = pandas_model(n=5, add=1, subtract=1)
#     loaded_pandas_model.audit(output_type="json", file="test-pandas.json", indent=2)
#     assert 0


def test_audit_xlsx(tmp_path):
    test_integer_out = os.path.join(tmp_path, "test-integers.xlsx")
    expected_integer_out = os.path.join("tests", "core", "data", "expected-integers.xlsx")
    integer_model = create_model("IntegerModel", steps=STEPS_USING_INTEGERS)
    loaded_integer_model = integer_model(a=1, b=1, c=1)
    loaded_integer_model.audit(output_type="xlsx", file=test_integer_out)
    assert compare_xlsx_files(test_integer_out, expected_integer_out, [], {})

    test_pandas_out = os.path.join(tmp_path, "test-pandas.xlsx")
    expected_pandas_out = os.path.join("tests", "core", "data", "expected-pandas.xlsx")
    pandas_model = create_model("PandasModel", steps=STEPS_USING_PANDAS)
    loaded_pandas_model = pandas_model(n=5, add=1, subtract=1)
    loaded_pandas_model.audit(output_type="xlsx", file=test_pandas_out)
    assert compare_xlsx_files(test_pandas_out, expected_pandas_out, [], {})
