"""Test for levels.py"""

# pylint: disable=function-redefined, missing-function-docstring

from footings.parameter import Parameter
from footings.schema import TblSchema, ColSchema
from footings.levels import TblStep, TblFlight, TblPlan
from footings.utils import  GET_PRIOR_STEP

def test_tbl_step():
    pass
    # step = TblStep(lambda df: "df", args={"df": "df"}, name="test")
    # print(step)
    # assert 1 == 2


def test_tbl_flight():
    pass


def test_tbl_plan():

    def pre_work(df):
        return df

    def partition(df, npartitions):
        return [df.assign(i=n) for n in range(npartitions)]

    def add(df):
        return df.assign(add=df.a + df.b)

    def subtract(df):
        return df.assign(subtract=df.a - df.b)

    def collapse(df):
        return df.groupby(["i"])["add", "subtract"].agg("sum")

    plan = TblPlan(
        "plan",
        [
            TblSchema("df", [ColSchema("a", int), ColSchema("b", int)]),
            TblStep(name="pre_work", function=pre_work, args={"df": GET_PRIOR_STEP}),
            TblStep(
                name="partition",
                function=partition,
                args={
                    "df": GET_PRIOR_STEP,
                    "npartitions": Parameter("npartitions", dtype=int, default=2),
                },
                partition=True,
            ),
            TblStep(
                name="add",
                function=add,
                args={"df": GET_PRIOR_STEP},
                required_columns=["a", "b"],
                added_columns=[ColSchema("add", int)],
            ),
            TblStep(
                name="subtract",
                function=subtract,
                args={"df": GET_PRIOR_STEP},
                required_columns=["a", "b"],
                added_columns=[ColSchema("subtract", int)],
            ),
            TblStep(
                name="collapse",
                function=collapse,
                args={"df": GET_PRIOR_STEP},
                required_columns=["i", "add", "subtract"],
                modified_columns=["add", "subtract"],
                collapse=True,
            ),
        ],
    )
    print(plan.to_task_graph())
