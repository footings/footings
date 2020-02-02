"""Tests for graph.py"""

import pandas as pd
from pandas.util.testing import assert_frame_equal
from dask.multiprocessing import get
import dask.dataframe as dd

from footings.core.graph import _build_graph_get_parameters, _create_graph_builder
from footings.core.ffunction import ffunction, ff_one_table, TableIn, TableOut
from footings.core.table_schema import TableSchema, ColumnSchema
from footings.core.parameter import Parameter

# pylint: disable=function-redefined, missing-function-docstring


def test_graph_builder():
    df_schema = TableSchema(
        "df", [ColumnSchema("a", dtype=int), ColumnSchema("b", dtype=int)]
    )

    @ff_one_table(df=TableIn("df", ["a", "b"]), added_columns=[ColumnSchema("i", str)])
    def pre_work(df):
        return df

    @ff_one_table(
        df=TableIn("df", []), npartitions=Parameter("npartitions", default=2), partition=True
    )
    def partition(df, npartitions):
        z = df.repartition(npartitions=npartitions)
        return z

    @ff_one_table(df=TableIn("df", ["a", "b"]), added_columns=[ColumnSchema("add", int)])
    def add(df):
        print(df)
        return df.assign(add=df.a + df.b)

    @ff_one_table(
        df=TableIn("df", ["a", "b"]), added_columns=[ColumnSchema("subtract", int)]
    )
    def subtract(df):
        return df.assign(subtract=df.a - df.b)

    @ff_one_table(df=TableIn("df", ["i"]), modified_columns=["a", "b", "add", "subtract"])
    def collapse(df):
        return df.groupby(["i"]).agg("sum")

    steps = [pre_work, partition, add]  # , subtract, collapse]

    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    z = _create_graph_builder(steps, ddf=dd.from_pandas(df, 1))

    from pprint import pprint

    # print(z.graph)
    # pprint(z.graph.__dask_graph__().dependencies)
    # pprint(z.graph.__dask_graph__().layers)

    # pprint(z.graph.dependencies)
    # pprint(z.graph.__dask_graph__().layers)
    # print(z.graph.compute())
    print(z.graph.compute())
    from dask.delayed import Delayed

    # test = Delayed("partition-zz", z.graph)
    # print(test.compute())
    assert 1 == 2


def test_graph():
    @ffunction(
        inputs={"df": TableIn("df", required_columns=["a", "b"])},
        outputs=TableOut("df", added_columns=[ColumnSchema("add", int)]),
    )
    def add(df):
        return df.assign(add=df.a + df.b)

    @ffunction(
        inputs={"df": TableIn("df", required_columns=["a", "b"])},
        outputs=TableOut("df", added_columns=[ColumnSchema("subtract", int)]),
    )
    def subtract(df):
        return df.assign(subtract=df.a - df.b)

    graph, params = _build_graph_get_parameters([add, subtract], "df")

    assert params == {}
    assert graph.graph == {
        "df": None,
        ("df", "add"): (add, "df"),
        ("df", "subtract"): (subtract, ("df", "add")),
    }
    assert graph.steps == [add, subtract]
    assert graph.keys == {"df": ("df", "subtract")}

    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    graph.update(df=df)

    assert graph.graph == {
        "df": df,
        ("df", "add"): (add, "df"),
        ("df", "subtract"): (subtract, ("df", "add")),
    }
    out = get(graph.graph, graph.keys["df"])
    assert_frame_equal(out, df.assign(add=df.a + df.b, subtract=df.a - df.b))
