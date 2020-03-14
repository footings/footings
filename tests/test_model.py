"""Test for model.py"""

# pylint: disable=function-redefined, missing-function-docstring
from inspect import getfullargspec

import pandas as pd
from pandas.testing import assert_frame_equal
import dask.dataframe as dd

from footings.parameter import Parameter
from footings.schema import TblSchema, ColSchema
from footings.levels import TblStep, TblPlan  # , TblFlight,
from footings.model import build_model
from footings.utils import GET_TBL, GET_PRIOR_STEP


def test_model():
    def pre_work(df):
        df["i"] = pd.Series(["a", "a", "b", "b"])
        return df

    plan = TblPlan(
        name="plan",
        tbl=TblSchema("df", [ColSchema("a", int), ColSchema("b", int)]),
        levels=[
            TblStep(
                name="pre_work",
                function=pre_work,
                args={"df": GET_TBL},
                added_columns=[ColSchema("i", str)],
            ),
            TblStep(
                name="partition",
                function=lambda df, npartitions: df.repartition(npartitions=npartitions),
                args={
                    "df": GET_PRIOR_STEP,
                    "npartitions": Parameter("npartitions", dtype=int, default=2),
                },
                partition=True,
            ),
            TblStep(
                name="add",
                function=lambda df: df.assign(add=df.a + df.b),
                args={"df": GET_PRIOR_STEP},
                required_columns=["a", "b"],
                added_columns=[ColSchema("add", int)],
            ),
            TblStep(
                name="subtract",
                function=lambda df: df.assign(subtract=df.a - df.b),
                args={"df": GET_PRIOR_STEP},
                required_columns=["a", "b"],
                added_columns=[ColSchema("subtract", int)],
            ),
            TblStep(
                name="collapse",
                function=lambda df: df.groupby(["i"]).agg({"add": sum, "subtract": sum}),
                args={"df": GET_PRIOR_STEP},
                returned_columns=[
                    ColSchema("i", str),
                    ColSchema("add", int),
                    ColSchema("subtract", int),
                ],
                collapse=True,
            ),
        ],
    )
    model = build_model("model", plan, "hello")
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [1, 2, 3, 4]})
    df_out = model(df=df, npartitions=2).run()
    test = pd.DataFrame(
        {"add": [6, 14], "subtract": [0, 0]}, index=pd.Index(["a", "b"], name="i")
    )
    assert_frame_equal(df_out, test)


#
# model = Model(df, nparameters).run(client=client)
# model = Model.using_shock_lapse(df, nparameters).run(client=client)
