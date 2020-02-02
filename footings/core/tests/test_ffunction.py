"""Tests for ffunction.py"""

import pytest
import pandas as pd
from pandas.util.testing import assert_frame_equal

from footings.core.table_schema import ColumnSchema
from footings.core.ffunction import (
    ColumnNotInTableError,
    ColumnInTableError,
    FFunctionExtraArgumentError,
    FFunctionMissingArgumentError,
    TableIn,
    TableOut,
    ffunction,
    ff_one_table,
)

# pylint: disable=function-redefined, missing-function-docstring


def test_table_in():
    test = TableIn(name="test", required_columns=["b", "c"])
    df1 = pd.DataFrame({"a": [1, 2]})
    pytest.raises(ColumnNotInTableError, test.check_valid, df1)
    df2 = df1.assign(b=[1, 2])
    pytest.raises(ColumnNotInTableError, test.check_valid, df2)
    df3 = df2.assign(c=[1, 2])
    assert test.check_valid(df3) is True


def test_table_out():
    df1 = pd.DataFrame({"a": [1, 2]})
    df2 = pd.DataFrame({"a": [1, 2], "b": [1, 2]})

    pytest.raises(TypeError, TableOut, "test", ColumnSchema("b"))
    test_added_columns = TableOut(name="test", added_columns=[ColumnSchema("b")])
    pytest.raises(ColumnNotInTableError, test_added_columns.check_valid, df1)
    assert test_added_columns.check_valid(df2) is True

    test_modified_columns = TableOut(name="test", modified_columns=["b"])
    pytest.raises(ColumnNotInTableError, test_modified_columns.check_valid, df1)
    assert test_modified_columns.check_valid(df2) is True

    test_removed_columns = TableOut(name="test", removed_columns=["b"])
    pytest.raises(ColumnInTableError, test_removed_columns.check_valid, df2)
    assert test_removed_columns.check_valid(df1) is True


def test_ffunction():
    def add(df):
        return df.assign(c=df.a + df.b)

    inputs = {"df": TableIn("df", required_columns=["a", "b"])}
    outputs = TableOut("df", added_columns=[ColumnSchema("c")])

    with pytest.raises(TypeError):
        # test passing inputs
        # ffunction(add, inputs=[], outputs=outputs)

        # @ffunction(inputs=[], outputs=outputs)
        # def add(a, b):
        #     return a + b

        ffunction(add, inputs=inputs["df"], outputs=outputs)

        @ffunction(inputs=inputs["df"], outputs=outputs)
        def add(df):
            return df.assign(c=df.a + df.b)

        ffunction(add, inputs={"df": []}, outputs=outputs)

        @ffunction(inputs={"df": []}, outputs=outputs)
        def add(df):
            return df.assign(c=df.a + df.b)

        # test passing outputs
        ffunction(add, inputs=inputs, outputs=[])

        @ffunction(inputs=inputs, outputs=[])
        def add(df):
            return df.assign(c=df.a + df.b)

        ffunction(add, inputs=inputs, outputs=None)

        @ffunction(inputs=inputs, outputs=None)
        def add(df):
            return df.assign(c=df.a + df.b)

    with pytest.raises(FFunctionMissingArgumentError):
        ffunction(add, inputs={"z": inputs["df"]}, outputs=outputs)

        @ffunction(inputs={"z": inputs["df"]}, outputs=outputs)
        def add(df):
            return df.assign(c=df.a + df.b)

    with pytest.raises(FFunctionExtraArgumentError):
        ffunction(add, inputs={"z": inputs["df"], **inputs}, outputs=outputs)

        @ffunction(inputs={"z": inputs["df"], **inputs}, outputs=outputs)
        def add(a, b):
            return a + b

    # need to test __call__
    df = pd.DataFrame({"a": [1, 2], "b": [1, 2]})
    f_add = ffunction(function=add, inputs=inputs, outputs=outputs)
    assert_frame_equal(f_add(df), df.assign(c=df.a + df.b))

    @ffunction(inputs=inputs, outputs=outputs)
    def add(df):
        return df.assign(c=df.a + df.b)

    assert_frame_equal(add(df), df.assign(c=df.a + df.b))

    # need to test generate_step
    print(f_add.get_step_items())
    assert f_add.get_step_items() == (("df",), f_add, ("df",))
    assert add.get_step_items() == (("df",), add, ("df",))

    # need to test docstring and args


def test_ff_one_table():
    def add(df):
        return df.assign(c=df.a + df.b)

    ff_one_table(add, df=TableIn("df", ["a", "b"]), added_columns=[ColumnSchema("c")])

    @ff_one_table(df=TableIn("df", ["a", "b"]), added_columns=[ColumnSchema("c")])
    def add(df):
        return df.assign(c=df.a + df.b)
