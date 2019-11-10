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


@pytest.fixture(scope="module")
def df():
    df = pd.DataFrame(
        {"t": [0, 1, 2, 3], "i": [0, 0.1, 0.09, 0.08], "cash": [1000, -350, -350, -350]}
    )
    df.set_index("t", inplace=True)
    return df


@pytest.fixture(scope="module")
def ddf(df):
    return dd.from_pandas(df, npartitions=1)


@pytest.fixture(scope="module")
def calcs():
    def calc_v(i: Column("float")) -> CReturn({"v": "float"}):
        return 1 / (1 + i)

    def calc_v_mode(
        i: Column("float"), mode: Setting(dtype="category", allowed=["A", "M"])
    ) -> CReturn({"v": "float"}):
        if mode == "A":
            return 1 / (1 + i)
        elif mode == "M":
            return 1 / (1 + i / 12)

    def calc_disc_factor(v: Column("float")) -> CReturn({"disc_factor": "float"}):
        return cumprod(v)

    def calc_disc_cash(
        cash: Column("float"), disc_factor: Column("float")
    ) -> CReturn({"disc_cash": "float"}):
        return cash * disc_factor

    def calc_pv(disc_cash: Column("float")) -> CReturn({"pv": "float"}):
        return cumsum(disc_cash)

    return calc_v, calc_v_mode, calc_disc_factor, calc_disc_cash, calc_pv


@pytest.fixture(scope="module")
def registry(calcs):
    calc_v, calc_v_mode, calc_disc_factor, calc_disc_cash, calc_pv = calcs
    registry_base = Registry(
        as_calculation(calc_v),
        as_calculation(calc_disc_factor),
        as_calculation(calc_disc_cash),
        as_calculation(calc_pv),
    )

    registry_mode = Registry(
        as_calculation(calc_v_mode),
        as_calculation(calc_disc_factor),
        as_calculation(calc_disc_cash),
        as_calculation(calc_pv),
    )
    return registry_base, registry_mode


class TestFFFunction:
    pass


# @pytest.mark.usefixtures("ddf", "registry")
class TestModelGraph:
    """"
    """

    def test_model_graph(self, ddf, registry):
        registry_base, registry_mode = registry
        model_graph_1 = ModelGraph(ddf._meta, registry_base)
        model_graph_2 = ModelGraph(ddf._meta, registry_mode)


class TestModelTemplate:
    def test_runtime_settings(self):
        pass

    def test_runtime_checks(self):
        pass

    def test_defined_settings(self):
        pass

    def test_scenario(self):
        pass

    def test_step(self):
        pass

    def test_instructions(self):
        pass

    def test_dask_functions(self):
        pass

    def test_dask_meta(self):
        pass


class TestModel:
    def test_model(self, df, ddf, registry, calcs):
        calc_v, calc_v_mode, calc_disc_factor, calc_disc_cash, calc_pv = calcs
        registry_base, registry_mode = registry

        m1_t = ModelTemplate(frame_meta=ddf._meta, registry=registry_base)
        m1_a = m1_t(frame=ddf)
        m1_b = Model(frame=ddf, registry=registry_base)

        test1 = df.assign(
            v=lambda x: calc_v(x["i"]),
            disc_factor=lambda x: calc_disc_factor(x["v"]),
            disc_cash=lambda x: calc_disc_cash(x["cash"], x["disc_factor"]),
            pv=lambda x: calc_pv(x["disc_cash"]),
        )

        assert m1_a.compute().equals(test1)  # assert_frame_equal(m1_a.compute(), test1)
        assert m1_b.compute().equals(test1)  # assert_frame_equal(m1_b.compute(), test1)

        m2_t = ModelTemplate(
            frame_meta=ddf._meta, registry=registry_mode, runtime_settings=["mode"]
        )
        m2_a = m2_t(frame=ddf, mode="A")
        m2_b = Model(frame=ddf, registry=registry_mode, mode="A")

        test2 = df.assign(
            v=lambda x: calc_v_mode(x["i"], mode="A"),
            disc_factor=lambda x: calc_disc_factor(x["v"]),
            disc_cash=lambda x: calc_disc_cash(x["cash"], x["disc_factor"]),
            pv=lambda x: calc_pv(x["disc_cash"]),
        )

        assert m2_a.compute().equals(test2)  # assert_frame_equal(m2_a.compute(), test2)
        assert m2_b.compute().equals(test2)  # assert_frame_equal(m2_b.compute(), test2)
