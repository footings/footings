from numpy import cumprod, cumsum
from pandas.util.testing import assert_frame_equal
import pandas as pd
import dask.dataframe as dd
import unittest

from footings import (
    Model,
    Registry,
    Calculation,
    as_calculation,
    Column,
    CReturn,
    Setting,
)
from footings.core.model import _build_model_graph, _get_functions, Model


class TestModel(unittest.TestCase):
    """
    """

    def test_model(self):

        df = pd.DataFrame(
            {
                "t": [0, 1, 2, 3],
                "i": [0, 0.1, 0.09, 0.08],
                "cash": [1000, -350, -350, -350],
            }
        )
        df.set_index("t", inplace=True)
        ddf = dd.from_pandas(df, npartitions=1)

        @as_calculation
        def calc_v(i: Column(float)) -> CReturn({"v": float}):
            return 1 / (1 + i)

        @as_calculation
        def calc_disc_factor(v: Column(float)) -> CReturn({"disc_factor": float}):
            return cumprod(v)

        @as_calculation
        def calc_disc_cash(
            cash: Column(float), disc_factor: Column(float)
        ) -> CReturn({"disc_cash": float}):
            return cash * disc_factor

        @as_calculation
        def calc_pv(disc_cash: Column(float)) -> CReturn({"pv": float}):
            return cumsum(disc_cash)

        reg = Registry(calc_v, calc_disc_factor, calc_disc_cash, calc_pv)
        m = Model(ddf, reg)
        test = df.assign(
            v=lambda x: calc_v(x["i"]),
            disc_factor=lambda x: calc_disc_factor(x["v"]),
            disc_cash=lambda x: calc_disc_cash(x["cash"], x["disc_factor"]),
            pv=lambda x: calc_pv(x["disc_cash"]),
        )
        assert_frame_equal(m.compute(), test)
