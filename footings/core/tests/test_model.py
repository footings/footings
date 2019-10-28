from numpy import cumprod, cumsum
from pandas.util.testing import assert_frame_equal
import pandas as pd
import dask.dataframe as dd
import unittest
import pytest
from pyarrow import Schema

from footings import (
    ModelTemplate,
    ModelFromTemplate,
    Model,
    as_model_template,
    Registry,
    Calculation,
    as_calculation,
    Column,
    CReturn,
    Setting,
)
from footings.core.model import _build_model_graph, _get_instructions, _to_ff_function


class TestFFFunction:
    pass


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

        reg1 = Registry(calc_v, calc_disc_factor, calc_disc_cash, calc_pv)
        m1 = Model(ddf, reg1)
        test = df.assign(
            v=lambda x: calc_v(x["i"]),
            disc_factor=lambda x: calc_disc_factor(x["v"]),
            disc_cash=lambda x: calc_disc_cash(x["cash"], x["disc_factor"]),
            pv=lambda x: calc_pv(x["disc_cash"]),
        )
        assert_frame_equal(m1.compute(), test)

        # test model with settings
        @as_calculation
        def calc_v_mode(
            i: Column(float), mode: Setting(dtype="category", allowed=["A", "M"])
        ) -> CReturn({"v": float}):
            if mode == "A":
                return 1 / (1 + i)
            elif mode == "M":
                return 1 / (1 + i / 12)

        reg2 = Registry(calc_v_mode, calc_disc_factor, calc_disc_cash, calc_pv)

        # test mode A
        m2_a = Model(ddf, reg2, settings={"mode": "A"})
        test2_a = df.assign(
            v=lambda x: calc_v_mode(x["i"], "A"),
            disc_factor=lambda x: calc_disc_factor(x["v"]),
            disc_cash=lambda x: calc_disc_cash(x["cash"], x["disc_factor"]),
            pv=lambda x: calc_pv(x["disc_cash"]),
        )
        assert_frame_equal(m2_a.compute(), test2_a)

        # test mode M
        m2_m = Model(ddf, reg2, settings={"mode": "M"})
        test2_m = df.assign(
            v=lambda x: calc_v_mode(x["i"], "M"),
            disc_factor=lambda x: calc_disc_factor(x["v"]),
            disc_cash=lambda x: calc_disc_cash(x["cash"], x["disc_factor"]),
            pv=lambda x: calc_pv(x["disc_cash"]),
        )
        assert_frame_equal(m2_m.compute(), test2_m)

        # test missing setting throws error
        schema = Schema.from_pandas(ddf._meta)
        pytest.raises(AssertionError, _build_model_graph, schema, reg2, {"mode1": "M"})

    def test_model_function(self):

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

        reg1 = Registry(calc_v, calc_disc_factor, calc_disc_cash, calc_pv)
        template = ModelTemplate(ddf._meta, reg1)
        model = ModelFromTemplate(ddf, template)
        test = df.assign(
            v=lambda x: calc_v(x["i"]),
            disc_factor=lambda x: calc_disc_factor(x["v"]),
            disc_cash=lambda x: calc_disc_cash(x["cash"], x["disc_factor"]),
            pv=lambda x: calc_pv(x["disc_cash"]),
        )
        assert_frame_equal(model.compute(), test)

    def test_as_model(self):

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
        def calc_v_mode(
            i: Column(float), mode: Setting(dtype="category", allowed=["A", "M"])
        ) -> CReturn({"v": float}):
            if mode == "A":
                return 1 / (1 + i)
            elif mode == "M":
                return 1 / (1 + i / 12)

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

        reg1 = Registry(calc_v_mode, calc_disc_factor, calc_disc_cash, calc_pv)

        # @as_model_template(frame=ddf._meta, registry=reg1)
        # def test_model(frame, mode):
        #     return ModelFromTemplate(frame, mode)
