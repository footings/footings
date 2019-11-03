from numpy import cumprod, cumsum
from pandas.util.testing import assert_frame_equal
import pandas as pd
import dask.dataframe as dd
import unittest
import pytest
from pyarrow import Schema

from footings.core.utils import _generate_message

from footings import (
    ModelTemplate,
    Model,
    Registry,
    Calculation,
    as_calculation,
    Column,
    CReturn,
    Setting,
)

from footings.core.model import ModelGraph, ModelDescription, ModelFromTemplate


class TestFFFunction:
    pass


class TestModelGraph:
    """"
    """

    def test_model_graph(self):
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

        reg1 = Registry(calc_v, calc_disc_factor, calc_disc_cash, calc_pv)
        model_graph_1 = ModelGraph(ddf._meta, reg1)
        model_temp_1 = ModelTemplate(frame_meta=ddf._meta, registry=reg1)

        reg2 = Registry(calc_v_mode, calc_disc_factor, calc_disc_cash, calc_pv)
        model_graph_2 = ModelGraph(ddf._meta, reg2)
        model_temp_2 = ModelTemplate(
            frame_meta=ddf._meta, registry=reg2, runtime_settings=["mode"]
        )


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
        m1_t = ModelTemplate(frame_meta=ddf._meta, registry=reg1)
        m1_a = m1_t(frame=ddf)
        m1_b = Model(frame=ddf, registry=reg1)

        test1 = df.assign(
            v=lambda x: calc_v(x["i"]),
            disc_factor=lambda x: calc_disc_factor(x["v"]),
            disc_cash=lambda x: calc_disc_cash(x["cash"], x["disc_factor"]),
            pv=lambda x: calc_pv(x["disc_cash"]),
        )
        assert_frame_equal(m1_a.compute(), test1)
        assert_frame_equal(m1_b.compute(), test1)

        # test model with settings a passed in settings
        @as_calculation
        def calc_v_mode(
            i: Column(float), mode: Setting(dtype="category", allowed=["A", "M"])
        ) -> CReturn({"v": float}):
            if mode == "A":
                return 1 / (1 + i)
            elif mode == "M":
                return 1 / (1 + i / 12)

        reg2 = Registry(calc_v_mode, calc_disc_factor, calc_disc_cash, calc_pv)
        m2_t = ModelTemplate(
            frame_meta=ddf._meta, registry=reg2, runtime_settings=["mode"]
        )
        m2_a = m2_t(frame=ddf, mode="A")
        m2_b = Model(frame=ddf, registry=reg2, mode="A")

        test2 = df.assign(
            v=lambda x: calc_v_mode(x["i"], mode="A"),
            disc_factor=lambda x: calc_disc_factor(x["v"]),
            disc_cash=lambda x: calc_disc_cash(x["cash"], x["disc_factor"]),
            pv=lambda x: calc_pv(x["disc_cash"]),
        )
        assert_frame_equal(m2_a.compute(), test2)
        assert_frame_equal(m2_b.compute(), test2)

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
        template = ModelTemplate(frame_meta=ddf._meta, registry=reg1)
        model = ModelFromTemplate(frame=ddf, template=template)
        test = df.assign(
            v=lambda x: calc_v(x["i"]),
            disc_factor=lambda x: calc_disc_factor(x["v"]),
            disc_cash=lambda x: calc_disc_cash(x["cash"], x["disc_factor"]),
            pv=lambda x: calc_pv(x["disc_cash"]),
        )
        assert_frame_equal(model.compute(), test)
