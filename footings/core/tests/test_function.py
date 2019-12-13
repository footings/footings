import pytest
import pandas as pd
import pyarrow as pa
from pandas.util.testing import assert_frame_equal

from footings import Setting, FFunction, ffunction, column_ffunction, dataframe_ffunction


def test_ffunction_raise_errors():
    pass


def test_ffunction_series_parse_true():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    def add1(a: pa.int16(), b: pa.int16()):
        return a + b

    f = FFunction(
        add1, return_type=pd.Series, parse_args=True, output_columns={"c": pa.int16()}
    )
    assert_frame_equal(f(df), df.assign(c=df.a + df.b))

    @ffunction(return_type=pd.Series, parse_args=True, output_columns={"c": pa.int16()})
    def add2(a: pa.int16(), b: pa.int16()):
        return a + b

    assert_frame_equal(add2(df), df.assign(c=df.a + df.b))

    @column_ffunction(output_columns={"c": pa.int16()})
    def add3(a: pa.int16(), b: pa.int16()):
        return a + b

    assert_frame_equal(add3(df), df.assign(c=df.a + df.b))


def test_ffunction_series_parse_false():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    def add1(a, b):
        return a + b

    f = FFunction(
        add1,
        return_type=pd.Series,
        parse_args=False,
        input_columns={"a": pa.int16(), "b": pa.int16()},
        output_columns={"c": pa.int16()},
    )
    assert_frame_equal(f(df), df.assign(c=df.a + df.b))

    @ffunction(
        return_type=pd.Series,
        parse_args=False,
        input_columns={"a": pa.int16(), "b": pa.int16()},
        output_columns={"c": pa.int16()},
    )
    def add2(a, b):
        return a + b

    assert_frame_equal(add2(df), df.assign(c=df.a + df.b))


def test_ffunction_properties_series():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    def func(a: pa.int16(), b: pa.int16(), s: Setting(allowed=["leave", "divide"])):
        if s == "leave":
            return a + b
        else:
            return (a + b) / 2

    ffunc = column_ffunction(func, output_columns={"c": pa.float64()})

    assert ffunc.function == func
    assert ffunc.name == func.__name__
    assert list(ffunc.settings.keys()) == ["s"]
    assert ffunc.drop_columns == None
    assert ffunc.output_columns == {"c": pa.float64()}

    assert_frame_equal(ffunc(df, s="leave"), df.assign(c=df.a + df.b))
    assert_frame_equal(ffunc(df, s="divide"), df.assign(c=(df.a + df.b) / 2))


def test_ffunction_dataframe():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    def add1(df):
        return df.assign(c=df.a + df.b)

    f = FFunction(
        add1,
        return_type=pd.DataFrame,
        parse_args=False,
        input_columns={"a": pa.int16(), "b": pa.int16()},
        output_columns={"c": pa.int16()},
    )
    assert_frame_equal(f(df), df.assign(c=df.a + df.b))

    @ffunction(
        parse_args=False,
        return_type=pd.DataFrame,
        input_columns={"a": pa.int16(), "b": pa.int16()},
        output_columns={"c": pa.int16()},
    )
    def add2(df):
        return df.assign(c=df.a + df.b)

    assert_frame_equal(add2(df), df.assign(c=df.a + df.b))


def test_ffunction_properties_dataframe():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "b2": [3, 4]})

    def func(df, s: Setting(allowed=["leave", "divide"])):
        if s == "leave":
            return df.assign(c=df.a + df.b).drop(columns="b2")
        else:
            return df.assign(c=(df.a + df.b) / 2).drop(columns="b2")

    ffunc = dataframe_ffunction(
        func,
        input_columns={"a": pa.int16(), "b": pa.int16(), "b2": pa.int16()},
        output_columns={"c": pa.float64()},
        drop_columns=["b2"],
    )

    assert ffunc.function == func
    assert ffunc.name == func.__name__
    assert list(ffunc.settings.keys()) == ["s"]
    assert ffunc.drop_columns == ["b2"]
    assert ffunc.output_columns == {"c": pa.float64()}

    assert_frame_equal(ffunc(df, s="leave"), df.assign(c=df.a + df.b).drop(columns="b2"))
    assert_frame_equal(
        ffunc(df, s="divide"), df.assign(c=(df.a + df.b) / 2).drop(columns="b2")
    )
