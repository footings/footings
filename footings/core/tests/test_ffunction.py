import pytest
import pandas as pd
import pyarrow as pa
from pandas.util.testing import assert_frame_equal

from footings import (
    Parameter,
    FFunction,
    ffunction,
    series_ffunction,
    dataframe_ffunction,
)
from footings.core.ffunction import to_dataframe_function


def test_ffunction_fail_return_type():
    def add(a, b):
        return a + b

    # fails > return_type is None
    pytest.raises(
        ValueError,
        FFunction,
        add,
        return_type=None,
        input_columns=["a", "b"],
        output_columns=[pa.field("c", pa.int16())],
    )

    # fails > return_type is not pd.Series or pd.DataFrame
    pytest.raises(
        ValueError,
        FFunction,
        add,
        return_type=pa.int16(),
        input_columns=["a", "b"],
        output_columns=[pa.field("c", pa.int16())],
    )


def test_ffunction_fail_input_columns():
    def add(a, b):
        return a + b

    # fails > input_columns is not a dict
    pytest.raises(
        TypeError,
        FFunction,
        add,
        return_type=pd.Series,
        input_columns=None,
        output_columns=[pa.field("c", pa.int16())],
    )

    # fails > the values for input_columns are not a pyarrow DataType
    pytest.raises(
        TypeError,
        FFunction,
        add,
        return_type=pd.Series,
        input_columns={"a": pd.Series, "b": pd.Series},
        output_columns=[pa.field("c", pa.int16())],
    )


def test_ffunction_fail_output_columns():
    def add(a, b):
        return a + b

    def func(a, b):
        return a + b, a - b

    # fails > output_columns is not a dict
    pytest.raises(
        TypeError,
        FFunction,
        add,
        return_type=pd.Series,
        input_columns=["a", "b"],
        output_columns=None,
    )

    # fails > the values for output_columns are not a pyarrow DataType
    pytest.raises(
        TypeError,
        FFunction,
        add,
        return_type=pd.Series,
        input_columns=["a", "b"],
        output_columns={"c": pa.int16()},
    )


def test_ffunction_fail_parameters():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    def func(df, s):
        if s == "leave":
            return df.assign(c=(df.a + df.b))
        else:
            return df.assign(c=(df.a + df.b) / 2)

    # fails > the values for input_columns are not a Parameter type
    pytest.raises(
        TypeError,
        FFunction,
        func,
        return_type=pd.Series,
        input_columns=["a", "b"],
        output_columns={"c": pd.Series},
        parameters={"s": str},
    )


def test_ffunction_fail_drop_columns():
    def add(a, b):
        return a + b

    # fails > drop_columns is not a list
    pytest.raises(
        TypeError,
        FFunction,
        add,
        return_type=pd.Series,
        input_columns=["a", "b"],
        output_columns=[pa.field("c", pa.int16())],
        drop_columns="b",
    )

    # fails > return_type == pd.Series
    pytest.raises(
        ValueError,
        FFunction,
        add,
        return_type=pd.Series,
        input_columns=["a", "b"],
        output_columns=[pa.field("c", pa.int16())],
        drop_columns=["b"],
    )

    # fails > drop_columns d not in input_columns
    pytest.raises(
        ValueError,
        FFunction,
        add,
        return_type=pd.DataFrame,
        input_columns=["a", "b"],
        output_columns=[pa.field("c", pa.int16())],
        drop_columns=["d"],
    )


def test_ffunction_series_pass():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    def add1(a, b):
        return a + b

    f = FFunction(
        add1,
        return_type=pd.Series,
        input_columns=["a", "b"],
        output_columns=[pa.field("c", pa.int16())],
    )
    assert_frame_equal(f(df), df.assign(c=df.a + df.b))

    @ffunction(
        return_type=pd.Series,
        input_columns=["a", "b"],
        output_columns=[pa.field("c", pa.int16())],
    )
    def add2(a, b):
        return a + b

    assert_frame_equal(add2(df), df.assign(c=df.a + df.b))

    @series_ffunction(
        input_columns=["a", "b"], output_columns=[pa.field("c", pa.int16())],
    )
    def add3(a, b):
        return a + b

    assert_frame_equal(add3(df), df.assign(c=df.a + df.b))


def test_ffunction_series_properties():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    def func(a, b, s):
        if s == "leave":
            return a + b
        else:
            return (a + b) / 2

    ffunc = FFunction(
        func,
        return_type=pd.Series,
        input_columns=["a", "b"],
        parameters={"s": Parameter(allowed=["leave", "divide"])},
        output_columns=[pa.field("c", pa.float64())],
    )

    assert ffunc.function == func
    assert ffunc._return_type == pd.Series
    assert ffunc.name == func.__name__
    assert list(ffunc.parameters.keys()) == ["s"]
    assert ffunc.drop_columns == None
    assert ffunc.output_columns == [pa.field("c", pa.float64())]

    assert_frame_equal(ffunc(df, s="leave"), df.assign(c=df.a + df.b))
    assert_frame_equal(ffunc(df, s="divide"), df.assign(c=(df.a + df.b) / 2))


def test_ffunction_dataframe_pass():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    def add1(df):
        return df.assign(c=df.a + df.b)

    f = FFunction(
        add1,
        return_type=pd.DataFrame,
        input_columns=["a", "b"],
        output_columns=[pa.field("c", pa.int16())],
    )
    assert_frame_equal(f(df), df.assign(c=df.a + df.b))

    @ffunction(
        return_type=pd.DataFrame,
        input_columns=["a", "b"],
        output_columns=[pa.field("c", pa.int16())],
    )
    def add2(df):
        return df.assign(c=df.a + df.b)

    assert_frame_equal(add2(df), df.assign(c=df.a + df.b))

    @dataframe_ffunction(
        input_columns=["a", "b"], output_columns=[pa.field("c", pa.int16())],
    )
    def add3(df):
        return df.assign(c=df.a + df.b)

    assert_frame_equal(add3(df), df.assign(c=df.a + df.b))


def test_ffunction_dataframe_properties():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "b2": [3, 4]})

    def func(df, s):
        if s == "leave":
            return df.assign(c=df.a + df.b).drop(columns="b2")
        else:
            return df.assign(c=(df.a + df.b) / 2).drop(columns="b2")

    ffunc = FFunction(
        func,
        return_type=pd.DataFrame,
        input_columns=["a", "b", "b2"],
        parameters={"s": Parameter(allowed=["leave", "divide"])},
        output_columns=[pa.field("c", pa.float64())],
        drop_columns=["b2"],
    )

    assert ffunc.function == func
    assert ffunc._return_type == pd.DataFrame
    assert ffunc.name == func.__name__
    assert list(ffunc.parameters.keys()) == ["s"]
    assert ffunc.drop_columns == ["b2"]
    assert ffunc.output_columns == [pa.field("c", pa.float64())]

    assert_frame_equal(ffunc(df, s="leave"), df.assign(c=df.a + df.b).drop(columns="b2"))
    assert_frame_equal(
        ffunc(df, s="divide"), df.assign(c=(df.a + df.b) / 2).drop(columns="b2")
    )


def test_to_dataframe_function():

    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    def add(a, b):
        return a + b

    add_df_func = to_dataframe_function(
        add, input_columns=["a", "b"], output_columns=[pa.field("add", pa.int16())],
    )

    assert_frame_equal(add_df_func(df), df.assign(add=df.a + df.b))

    def add_subtract(a, b):
        return a + b, b - a

    add_subtract_df_func = to_dataframe_function(
        add_subtract,
        input_columns=["a", "b"],
        output_columns=[pa.field("add", pa.int16()), pa.field("subtract", pa.int16())],
    )

    assert_frame_equal(
        add_subtract_df_func(df), df.assign(add=df.a + df.b, subtract=df.b - df.a)
    )


# def test_ffunction_repr():
#     def func(df, s):
#         if s == "leave":
#             return df.assign(c=df.a + df.b).drop(columns="b2")
#         else:
#             return df.assign(c=(df.a + df.b) / 2).drop(columns="b2")
#
#     ffunc = FFunction(
#         func,
#         return_type=pd.DataFrame,
#         input_columns={"a": pa.int16(), "b": pa.int16(), "b2": pa.int16()},
#         parameters={"s": Parameter(allowed=["leave", "divide"])},
#         output_columns=[pa.field("c", pa.float64())],
#         drop_columns=["b2"],
#     )
#
#     print(ffunc)


# def test_doc_description():
#     @series_ffunction(
#         input_columns=["a", "b"],
#         output_columns=[pa.field("c", pa.int16())],
#     )
#     def series_add(a, b):
#         """Add columns a and b together to produce column c"""
#         return a + b
#
#     print(series_add._function.__doc__)
#     assert series_add.__doc__ == "Add columns a and b together to produce column c"
#
#     @dataframe_ffunction(
#         input_columns=["a", "b"],
#         output_columns=[pa.field("c", pa.int16())],
#     )
#     def df_add(df):
#         """Add columns a and b together to produce column c"""
#         return df.assign(c=df.a + df.b)
#
#     print(df_add._function.__doc__)
#     assert df_add.__doc__ == "Add columns a and b together to produce column c"
