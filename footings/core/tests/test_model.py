import pytest
from pandas.util.testing import assert_frame_equal
import pandas as pd
from collections import namedtuple

from footings import series_ffunction, dataframe_ffunction, FFunction, build_model

from footings.core.ffunction import DFIn, DFOut
from footings.core.parameter import Parameter
from footings.core.table_schema import ColumnSchema, TableSchema


def test_build_model_tables1_params0():
    def add(df):
        return df.assign(add=df.a + df.b)

    def subtract(df):
        return df.assign(subtract=df.a - df.b)

    model_name = "model"
    table_ab = TableSchema(
        "table_ab", [ColumnSchema("a", dtype=int), ColumnSchema("b", dtype=int)]
    )
    instructions = {
        "table_ab": None,
        ("table_ab", "add"): (add, "table_ab"),
        ("table_ab", "subtract"): (subtract, ("table_ab", "add")),
    }
    keys = [("table_ab", "subtract")]
    model = build_model(model_name, table_ab, instructions, keys)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    test = df.assign(add=(df.a + df.b), subtract=(df.a - df.b))
    assert_frame_equal(model(table_ab=df).compute(), test)


def test_build_model_tables1_params1():
    def add(df, divide_by):
        return df.assign(add=(df.a + df.b) / divide_by)

    def subtract(df, divide_by):
        return df.assign(subtract=(df.a - df.b) / divide_by)

    model_name = "model"
    table_ab = TableSchema(
        "table_ab", [ColumnSchema("a", dtype=int), ColumnSchema("b", dtype=int)]
    )
    instructions = {
        "table_ab": None,
        "parameter_divide_by": Parameter("parameter_divide_by"),
        ("table_ab", "add"): (add, "table_ab", "parameter_divide_by"),
        ("table_ab", "subtract"): (subtract, ("table_ab", "add"), "parameter_divide_by"),
    }
    keys = [("table_ab", "subtract")]
    model = build_model(model_name, table_ab, instructions, keys)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    test = df.assign(add=(df.a + df.b) / 2, subtract=(df.a - df.b) / 2)
    assert_frame_equal(model(table_ab=df, parameter_divide_by=2).compute(), test)


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
    table_ab = TableSchema(
        "table_ab", [ColumnSchema("a", dtype=int), ColumnSchema("b", dtype=int)]
    )
    instructions = {
        "table_ab": None,
        "parameter_divide_by": Parameter("parameter_divide_by"),
        "parameter_apply_divide": Parameter("parameter_apply_divide"),
        ("table_ab", "add"): (
            add,
            "table_ab",
            "parameter_divde_by",
            "parameter_apply_divide",
        ),
        ("table_ab", "subtract"): (
            subtract,
            ("table_ab", "add"),
            "parameter_divde_by",
            "parameter_apply_divide",
        ),
    }
    key = [("table_ab", "subtract")]
    model = build_model(model_name, table_ab, instructions, key)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    test = df.assign(add=(df.a + df.b), subtract=(df.a - df.b))
    assert_frame_equal(
        model(table_ab=df, parameter_divide_by=2, parameter_apply_divide=False).compute(),
        test,
    )


# def test_build_model_tables2_params0():
#     def add(df, divide_by, apply_divide):
#         if apply_divide:
#             return df.assign(add=(df.a + df.b) / divide_by)
#         else:
#             return df.assign(add=(df.a + df.b))
#
#     def subtract(df, divide_by, apply_divide):
#         if apply_divide:
#             return df.assign(subtract=(df.a - df.b) / divide_by)
#         else:
#             return df.assign(subtract=(df.a - df.b))
#
#     model_name = "model"
#     table_ab = TableSchema("table_ab", [
#         ColumnSchema("a", dtype=int),
#         ColumnSchema("b", dtype=int)
#     ])
#     instructions = {
#         "table_ab": None,
#         "parameter_divide_by": Parameter("parameter_divide_by"),
#         "parameter_apply_divide": Parameter("parameter_apply_divide"),
#         ("table_ab", "add"): (
#             add,
#             "table_ab",
#             "parameter_divde_by",
#             "parameter_apply_divide",
#         ),
#         ("table_ab", "subtract"): (
#             subtract,
#             ("table_ab", "add"),
#             "parameter_divde_by",
#             "parameter_apply_divide",
#         ),
#     }
#     key = [("table_ab", "subtract")]
#     model = build_model(model_name, table_ab, instructions, key)
#     print(model.__footings_functions__)
#     assert 1 == 2
#


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
