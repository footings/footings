"""Test for audit.py"""

# pylint: disable=function-redefined, missing-function-docstring

# from footings import build_model
#
# from .shared import steps_using_integers, steps_using_pandas
#
#
# def test_audit_json():
#     integer_model = build_model("IntegerModel", steps=steps_using_integers())
#     loaded_integer_model = integer_model(a=1, b=1, c=1)
#     print(loaded_integer_model)
#     # loaded_integer_model.audit(output_type="json", file="test-integers.json", indent=2)
#
#     pandas_model = build_model("PandasModel", steps=steps_using_pandas())
#     loaded_pandas_model = pandas_model(n=5, add=1, subtract=1)
#     print(loaded_pandas_model)
#     # z = loaded_pandas_model.audit(output_type="json", file="test-pandas.json", indent=2)
#     # assert 0
#
#
# def test_audit_xlsx():
#     integer_model = build_model("IntegerModel", steps=steps_using_integers())
#     loaded_integer_model = integer_model(a=1, b=1, c=1)
#     print(loaded_integer_model)
#     # loaded_integer_model.audit(output_type="xlsx", file="test-integers.xlsx", indent=2)
#
#     pandas_model = build_model("PandasModel", steps=steps_using_pandas())
#     loaded_pandas_model = pandas_model(n=5, add=1, subtract=1)
#     print(loaded_pandas_model)
#     # z = loaded_pandas_model.audit(output_type="xlsx", file="test-pandas.xlsx", indent=2)
#     # assert 0
