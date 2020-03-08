"""test for functions.py"""

# pylint: disable=function-redefined, missing-function-docstring

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from footings.functions import (
    FootingsNestedFunctionError,
    FootingsTblFunctionExitError,
    FootingsTblFunctionEnterError,
    FootingsTblFunctionCreationError,
    FootingsTblFunctionCallError,
    NestedFunction,
    make_nested,
    TblFunction,
)

from footings.schema import TblSchema, ColSchema


def test_nested_function():
    df = pd.DataFrame({"a": [1, 2, 3]})

    def primary(df):
        df = df.copy()
        df["intermediate"] = df["a"] * 2
        df["final"] = df["intermediate"] + df.a
        return df

    def post_primary(df):
        return df.drop(["intermediate"], axis=1)

    test_1 = NestedFunction(function=primary, post_function=post_primary)
    assert_frame_equal(test_1(df), df.assign(final=df["a"] * 3))

    test_2 = make_nested(primary, custom=post_primary)
    assert_frame_equal(test_2(df), df.assign(final=df["a"] * 3))

    test_3 = make_nested(primary, remove_columns=["intermediate"])
    assert_frame_equal(test_3(df), df.assign(final=df["a"] * 3))

    @make_nested(custom=post_primary)
    def test_4(df):
        df = df.copy()
        df["intermediate"] = df["a"] * 2
        df["final"] = df["intermediate"] + df.a
        return df

    assert_frame_equal(test_4(df), df.assign(final=df["a"] * 3))

    @make_nested(remove_columns=["intermediate"])
    def test_5(df):
        df = df.copy()
        df["intermediate"] = df["a"] * 2
        df["final"] = df["intermediate"] + df.a
        return df

    assert_frame_equal(test_5(df), df.assign(final=df["a"] * 3))

    pytest.raises(
        FootingsNestedFunctionError,
        make_nested,
        function=primary,
        custom=None,
        remove_columns=None,
    )
    pytest.raises(
        FootingsNestedFunctionError,
        make_nested,
        function=primary,
        custom=post_primary,
        remove_columns=["intermediate"],
    )
    pytest.raises(
        FootingsNestedFunctionError,
        NestedFunction,
        function=primary,
        post_function=lambda df, x: df,
    )


def test_tbl_function():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [2, 4, 6]})

    def add_col_b(df):
        return df.assign(a2=df.a * 2).drop(["b"], axis=1)

    test_pass_1 = TblFunction.create(
        "df",
        add_col_b,
        required_columns=["a"],
        removed_columns=["b"],
        added_columns=[ColSchema("a2", int)],
    )
    assert_frame_equal(test_pass_1(df=df), add_col_b(df))

    test_pass_2 = TblFunction.create(
        "df",
        add_col_b,
        required_columns=["a"],
        returned_schema=TblSchema(
            name="test", columns=[ColSchema("a", int), ColSchema("a2", int)]
        ),
    )
    assert_frame_equal(test_pass_2(df=df), add_col_b(df))

    # required_column not present on enter
    test_required_1 = TblFunction.create(
        "df",
        add_col_b,
        required_columns=["a", "c"],
        removed_columns=["b"],
        added_columns=[ColSchema("a2", int)],
    )
    pytest.raises(FootingsTblFunctionEnterError, test_required_1, df=df)

    # removed_column not present on enter
    test_removed_1 = TblFunction.create(
        "df",
        add_col_b,
        required_columns=["a"],
        removed_columns=["b", "c"],
        added_columns=[ColSchema("a2", int)],
    )
    pytest.raises(FootingsTblFunctionEnterError, test_removed_1, df=df)

    # removed_column still present on exit
    test_removed_2 = TblFunction.create(
        "df",
        lambda df: df.assign(a2=df.a * 2),
        required_columns=["a"],
        removed_columns=["b"],
        added_columns=[ColSchema("a2", int)],
    )
    pytest.raises(FootingsTblFunctionExitError, test_removed_2, df=df)

    # column removed but not mentioned in removed_column
    test_removed_3 = TblFunction.create(
        "df", add_col_b, required_columns=["a"], added_columns=[ColSchema("a2", int)]
    )
    pytest.raises(FootingsTblFunctionExitError, test_removed_3, df=df)

    # added_columns present on enter
    test_added_1 = TblFunction.create(
        "df",
        add_col_b,
        required_columns=["a"],
        removed_columns=["b"],
        added_columns=[ColSchema("a", int)],
    )
    pytest.raises(FootingsTblFunctionEnterError, test_added_1, df=df)

    # added_columns not present on exit
    test_added_2 = TblFunction.create(
        "df",
        add_col_b,
        required_columns=["a"],
        removed_columns=["b"],
        added_columns=[ColSchema("a2", int), ColSchema("a3", int)],
    )
    pytest.raises(FootingsTblFunctionExitError, test_added_2, df=df)

    # column added but not mentioned in added_column
    test_added_3 = TblFunction.create(
        "df",
        lambda df: df.assign(a2=df.a * 2, a3=df.a * 3),
        required_columns=["a"],
        added_columns=[ColSchema("a2", int)],
    )
    pytest.raises(FootingsTblFunctionExitError, test_added_3, df=df)

    # returned_columns not present on enter
    test_returned_cols_1 = TblFunction.create(
        "df",
        add_col_b,
        required_columns=["a"],
        added_columns=[ColSchema("a2", int)],
        returned_columns=["a", "a2", "c"],
    )
    pytest.raises(FootingsTblFunctionEnterError, test_returned_cols_1, df=df)

    # returned_columns not_present on exit
    test_returned_cols_2 = TblFunction.create(
        "df",
        add_col_b,
        required_columns=["a"],
        added_columns=[ColSchema("a2", int)],
        returned_columns=["a", "b", "a2"],
    )
    pytest.raises(FootingsTblFunctionExitError, test_returned_cols_2, df=df)

    # returned_schema
    test_returned_schema_1 = TblFunction.create(
        "df",
        add_col_b,
        required_columns=["a"],
        returned_schema=TblSchema("test", [ColSchema("a", int), ColSchema("b", int)]),
    )
    pytest.raises(FootingsTblFunctionExitError, test_returned_schema_1, df=df)
