from pandas.util.testing import assert_frame_equal
import numpy as np
import pandas as pd
import pyarrow as pa
import dask.dataframe as dd
import unittest
import pytest
from collections import namedtuple
from inspect import getfullargspec

from footings import (
    Parameter,
    series_ffunction,
    dataframe_ffunction,
    FFunction,
    build_model,
)

# from footings.core.model import _instructions_to_dict, _get_graph_fields_keys
from footings.core.ffunction import DFIn, DFOut
from footings.core.errors import ParameterDuplicateError

# from footings.core.utils import _generate_message


def test_build_model_tables1_params0():
    def add(df):
        return df.assign(add=df.a + df.b)

    def subtract(df):
        return df.assign(subtract=df.a - df.b)

    model_name = "model"
    table_schemas = {"table_df": {"a": int, "b": int}}
    instructions = {
        "table_df": table_schemas["table_df"],
        ("table_df", "add"): (add, "table_df"),
        ("table_df", "subtract"): (subtract, ("table_df", "add")),
    }
    keys = [("table_df", "subtract")]
    model = build_model(model_name, table_schemas, instructions, keys)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    test = df.assign(add=(df.a + df.b), subtract=(df.a - df.b))
    assert_frame_equal(model(table_df=df).compute(), test)


def test_build_model_tables1_params1():
    def add(df, divide_by):
        return df.assign(add=(df.a + df.b) / divide_by)

    def subtract(df, divide_by):
        return df.assign(subtract=(df.a - df.b) / divide_by)

    model_name = "model"
    table_schemas = {"table_df": {"a": int, "b": int}}
    instructions = {
        "table_df": table_schemas["table_df"],
        "parameter_divide_by": Parameter("parameter_divide_by"),
        ("table_df", "add"): (add, "table_df", "parameter_divide_by"),
        ("table_df", "subtract"): (subtract, ("table_df", "add"), "parameter_divide_by"),
    }
    keys = [("table_df", "subtract")]
    model = build_model(model_name, table_schemas, instructions, keys)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    test = df.assign(add=(df.a + df.b) / 2, subtract=(df.a - df.b) / 2)
    assert_frame_equal(model(table_df=df, parameter_divide_by=2).compute(), test)


def test_build_model_tables1_params2():
    def add(df, divide_by, apply_divide):
        if apply_divide:
            return df.assign(add=(df.a + df.b) / divide_by)
        else:
            return df.assign(add=(df.a + df.b))

    def subtract(df, divide_by, apply_divide):
        if apply_divide:
            return df.assign(subtract=(df.a - df.b) / divide_by)
        else:
            return df.assign(subtract=(df.a - df.b))

    model_name = "model"
    table_schemas = {"table_df": {"a": int, "b": int}}
    instructions = {
        "table_df": table_schemas["table_df"],
        "parameter_divide_by": Parameter("parameter_divide_by"),
        "parameter_apply_divide": Parameter("parameter_apply_divide"),
        ("table_df", "add"): (
            add,
            "table_df",
            "parameter_divde_by",
            "parameter_apply_divide",
        ),
        ("table_df", "subtract"): (
            subtract,
            ("table_df", "add"),
            "parameter_divde_by",
            "parameter_apply_divide",
        ),
    }
    key = [("table_df", "subtract")]
    model = build_model(model_name, table_schemas, instructions, key)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    test = df.assign(add=(df.a + df.b), subtract=(df.a - df.b))
    assert_frame_equal(
        model(table_df=df, parameter_divide_by=2, parameter_apply_divide=False).compute(),
        test,
    )


def test_build_model_tables2_params0():
    def add(df, divide_by, apply_divide):
        if apply_divide:
            return df.assign(add=(df.a + df.b) / divide_by)
        else:
            return df.assign(add=(df.a + df.b))

    def subtract(df, divide_by, apply_divide):
        if apply_divide:
            return df.assign(subtract=(df.a - df.b) / divide_by)
        else:
            return df.assign(subtract=(df.a - df.b))

    model_name = "model"
    table_schemas = {"table_df": {"a": int, "b": int}}
    instructions = {
        "table_df": table_schemas["table_df"],
        "parameter_divide_by": Parameter("parameter_divide_by"),
        "parameter_apply_divide": Parameter("parameter_apply_divide"),
        ("table_df", "add"): (
            add,
            "table_df",
            "parameter_divde_by",
            "parameter_apply_divide",
        ),
        ("table_df", "subtract"): (
            subtract,
            ("table_df", "add"),
            "parameter_divde_by",
            "parameter_apply_divide",
        ),
    }
    key = [("table_df", "subtract")]
    model = build_model(model_name, table_schemas, instructions, key)
    print(model.__footings_functions__)
    assert 1 == 2


def test_build_model_tables2_params1():
    pass


def test_build_model_tables2_params2():
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
