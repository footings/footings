from pandas.util.testing import assert_frame_equal
import numpy as np
import pandas as pd
import pyarrow as pa
import dask.dataframe as dd
import unittest
import pytest
from collections import namedtuple

from footings import (
    Parameter,
    series_ffunction,
    dataframe_ffunction,
    FFunction,
    Model,
)

from footings.core.ffunction import DFIn, DFOut

from footings.core.model import create_dask_graph
from footings.core.errors import ParameterDuplicateError

# from footings.core.utils import _generate_message


def test_schemas():
    pass


@pytest.fixture
def funcs():
    @series_ffunction(inputs=DFIn("df", ["a", "b"]), outputs=DFOut("df", [("add", int)]))
    def series_add(a, b):
        return a + b

    @series_ffunction(
        inputs=DFIn("df", ["a", "b"]), outputs=DFOut("df", [("subtract", int)])
    )
    def series_subtract(a, b):
        return a - b

    @dataframe_ffunction(
        inputs=DFIn("df", ["a", "b"]), outputs=DFOut("df", [("add", int)])
    )
    def dataframe_add(df):
        return df.assign(add=df.a + df.b)

    @dataframe_ffunction(
        inputs=DFIn({"df": "zz"}, ["a", "b"]), outputs=DFOut("df", [("subtract", int)])
    )
    def dataframe_subtract(zz):
        return zz.assign(subtract=zz.a - zz.b)

    @series_ffunction(
        inputs=[DFIn("df", ["a", "b"]), Parameter("divide_by", int)],
        outputs=DFOut("df", [("add_divide_by", int)]),
    )
    def series_add_divide_by(a, b, divide_by):
        return (a + b) / divide_by

    @series_ffunction(
        inputs=[DFIn("df", ["a", "b"]), Parameter("divide_by", int)],
        outputs=DFOut("df", [("subtract_divide_by", int)]),
    )
    def series_subtract_divide_by(a, b, divide_by):
        return (a - b) / divide_by

    @dataframe_ffunction(
        inputs=[DFIn("df", ["a", "b"]), Parameter("divide_by", int)],
        outputs=DFOut("df", [("add", int)]),
    )
    def dataframe_add_divide_by(df, divide_by):
        return df.assign(add_divide_by=(df.a + df.b) / divide_by)

    @dataframe_ffunction(
        inputs=[DFIn({"df": "zz"}, ["a", "b"]), Parameter("divide_by", int)],
        outputs=DFOut("df", [("subtract", int)]),
    )
    def dataframe_subtract_divide_by(zz, divide_by):
        return zz.assign(subtract_divide_by=(zz.a - zz.b) / divide_by)

    @series_ffunction(
        inputs=[
            DFIn("df", ["a", "b"]),
            Parameter("divide_by", int),
            Parameter("apply_divide", bool),
        ],
        outputs=DFOut("df", [("add_op_divide_by", int)]),
    )
    def series_add_op_divide_by(a, b, divide_by, apply_divide):
        if apply_divide:
            return (a + b) / divide_by
        else:
            return a + b

    @series_ffunction(
        inputs=[
            DFIn("df", ["a", "b"]),
            Parameter("divide_by", int),
            Parameter("apply_divide", bool),
        ],
        outputs=DFOut("df", [("subtract_op_divide_by", int)]),
    )
    def series_subtract_op_divide_by(a, b, divide_by, apply_divide):
        if apply_divide:
            return (a - b) / divide_by
        else:
            return a + b

    @dataframe_ffunction(
        inputs=[
            DFIn("df", ["a", "b"]),
            Parameter("divide_by", int),
            Parameter("apply_divide", bool),
        ],
        outputs=DFOut("df", [("add", int)]),
    )
    def dataframe_add_op_divide_by(df, divide_by, apply_divide):
        if apply_divide:
            return df.assign(add_op_divide_by=(df.a + df.b) / divide_by)
        else:
            return df.assign(add_op_divide_by=df.a + df.b)

    @dataframe_ffunction(
        inputs=[
            DFIn({"df": "zz"}, ["a", "b"]),
            Parameter("divide_by", int),
            Parameter("apply_divide", bool),
        ],
        outputs=DFOut("df", [("subtract", int)]),
    )
    def dataframe_subtract_op_divide_by(zz, divide_by, apply_divide):
        if apply_divide:
            return zz.assign(subtract_op_divide_by=(zz.a + zz.b) / divide_by)
        else:
            return zz.assign(subtract_op_divide_by=zz.a + zz.b)

    Funcs = namedtuple(
        "Funcs",
        [
            "series_add",
            "series_subtract",
            "dataframe_add",
            "dataframe_subtract",
            "series_add_divide_by",
            "series_subtract_divide_by",
            "dataframe_add_divide_by",
            "dataframe_subtract_divide_by",
            "series_add_op_divide_by",
            "series_subtract_op_divide_by",
            "dataframe_add_op_divide_by",
            "dataframe_subtract_op_divide_by",
        ],
    )
    return Funcs(
        series_add=series_add,
        series_subtract=series_subtract,
        dataframe_add=dataframe_add,
        dataframe_subtract=dataframe_subtract,
        series_add_divide_by=series_add_divide_by,
        series_subtract_divide_by=series_subtract_divide_by,
        dataframe_add_divide_by=dataframe_add_divide_by,
        dataframe_subtract_divide_by=dataframe_subtract_divide_by,
        series_add_op_divide_by=series_add_op_divide_by,
        series_subtract_op_divide_by=series_subtract_op_divide_by,
        dataframe_add_op_divide_by=dataframe_add_op_divide_by,
        dataframe_subtract_op_divide_by=dataframe_subtract_op_divide_by,
    )


def test_functions(funcs):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    assert_frame_equal(funcs.series_add(df), df.assign(add=df.a + df.b))
    assert_frame_equal(funcs.series_subtract(df), df.assign(subtract=df.a - df.b))
    assert_frame_equal(funcs.dataframe_add(df), df.assign(add=df.a + df.b))
    assert_frame_equal(funcs.dataframe_subtract(df), df.assign(subtract=df.a - df.b))

    assert_frame_equal(
        funcs.series_add_divide_by(df, divide_by=2),
        df.assign(add_divide_by=(df.a + df.b) / 2),
    )
    assert_frame_equal(
        funcs.series_subtract_divide_by(df, divide_by=2),
        df.assign(subtract_divide_by=(df.a - df.b) / 2),
    )
    assert_frame_equal(
        funcs.dataframe_add_divide_by(df, divide_by=2),
        df.assign(add_divide_by=(df.a + df.b) / 2),
    )
    assert_frame_equal(
        funcs.dataframe_subtract_divide_by(df, divide_by=2),
        df.assign(subtract_divide_by=(df.a - df.b) / 2),
    )

    # assert_frame_equal(funcs.series_add_op_divide_by(df), df.assign(add=df.a + df.b))
    # assert_frame_equal(
    #     funcs.series_subtract_op_divide_by(df), df.assign(subtract=df.a - df.b)
    # )
    # assert_frame_equal(funcs.dataframe_add_op_divide_by(df), df.assign(add=df.a + df.b))
    # assert_frame_equal(
    #     funcs.dataframe_subtract_op_divide_by(df), df.assign(add=df.a - df.b)
    # )


def test_create_dask_graph():
    df_schema = pa.schema([("a", pa.int16()), ("b", pa.int16())])

    def add(a, b):
        return a + b

    f_add = FFunction(
        add,
        pd.Series,
        inputs=[DFIn("df", ["a", "b"])],
        outputs=DFOut("df", [pa.field("add", pa.int16())], None, None),
    )

    def subtract(a, b):
        return a - b

    f_subtract = FFunction(
        subtract,
        pd.Series,
        inputs=[DFIn("df", ["a", "b"])],
        outputs=DFOut("df", [pa.field("subtract", pa.int16())], None, None),
    )

    schemas = {"df": "xxx"}
    steps = [f_add, f_subtract]
    dask_graph = create_dask_graph(schemas, steps, {})
    print(dask_graph)
    assert list(dask_graph.keys()) == ["df", "df-add", "df-subtract"]


def test_model_no_parameters():
    df_schema = pa.schema([("a", pa.int16()), ("b", pa.int16())])

    schemas = {"df": df_schema}
    steps = [f_add, f_subtract]
    key = "df-subtract"

    model = Model(schemas=schemas, steps=steps, key=key)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    z = model(df=df)  # .compute())
    z.compute()
    assert 1 == 2


# def test_model_runtime_parameters():
#     schema = pa.schema([("a", pa.int16()), ("b", pa.int16())])
#
#     @series_ffunction(
#         input_columns=[],
#         parameters={"v": Parameter()},
#         output_columns=[pa.field("v1", pa.int16())],
#     )
#     def add_v(v):
#         return v
#
#     @series_ffunction(
#         input_columns=[],
#         parameters={"v": Parameter(default=1)},
#         output_columns=[pa.field("v2", pa.int16())],
#     )
#     def add_v2():
#         return v
#
#     model = Model(schemas={"df": schema}, steps=[add_v, add_v2])
#     print(model.steps)
#     print(model.parameters)
#     assert 2 == 1
#
#
# def test_defined_parameters():
#     pass
#
#
# def test_defined_runtime_parameters():
#     pass
#
#
