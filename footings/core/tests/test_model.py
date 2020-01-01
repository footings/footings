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
    Model,
)

from footings.core.model import create_dask_graph
from footings.core.errors import DuplicateParameterError

# from footings.core.utils import _generate_message


def test_schemas():
    pass


def test_create_dask_graph():
    def add(df):
        return df.assign(add=df.a + df.b)

    def subtract(df):
        return df.assign(subtract=df.a - df.b)

    schemas = {"df": "xxx"}
    graph = {"df_add": (add, "df"), "output": (subtract, "df_add")}
    dask_graph = create_dask_graph(graph, schemas)
    print(dask_graph)
    assert dask_graph == {"df": None, **graph}


def test_model_no_parameters():
    df_schema = pa.schema([("a", pa.int16()), ("b", pa.int16())])

    @series_ffunction(
        input_columns=["a", "b"], output_columns=[pa.field("add", pa.int16())]
    )
    def add(a, b):
        return a + b

    @series_ffunction(
        input_columns=["a", "b"], output_columns=[pa.field("subtract", pa.int16())]
    )
    def subtract(a, b):
        return a - b

    schemas = {"df": df_schema}
    graph = {"df_add": (add, "df"), "output": (subtract, "df_add")}
    key = "output"

    model = Model(schemas=schemas, graph=graph, key=key)


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
