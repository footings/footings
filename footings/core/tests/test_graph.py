"""Tests for graph.py"""

import pandas as pd
from pandas.util.testing import assert_frame_equal
from dask.multiprocessing import get

from footings.core.graph import _build_graph_get_parameters
from footings.core.ffunction import ffunction, TableIn, TableOut
from footings.core.table_schema import ColumnSchema

# from footings.core.parameter import Parameter


def test_graph():
    """Test Graph"""

    @ffunction(
        inputs={"df": TableIn("df", required_columns=["a", "b"])},
        outputs=TableOut("df", added_columns=[ColumnSchema("add")]),
    )
    def add(df):
        return df.assign(add=df.a + df.b)

    @ffunction(
        inputs={"df": TableIn("df", required_columns=["a", "b"])},
        outputs=TableOut("df", added_columns=[ColumnSchema("subtract")]),
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
