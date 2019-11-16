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

from footings.core.model import ModelDescription, ModelFromTemplate


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

    return {
        "calc_v": calc_v,
        "calc_v_mode": calc_v_mode,
        "calc_disc_factor": calc_disc_factor,
        "calc_disc_cash": calc_disc_cash,
        "calc_pv": calc_pv,
    }


@pytest.fixture(scope="module")
def registry(ddf, calcs):

    registry_base = Registry(
        as_calculation(calcs["calc_v"]),
        as_calculation(calcs["calc_disc_factor"]),
        as_calculation(calcs["calc_disc_cash"]),
        as_calculation(calcs["calc_pv"]),
        frame=ddf._meta,
    )

    registry_mode = Registry(
        as_calculation(calcs["calc_v_mode"]),
        as_calculation(calcs["calc_disc_factor"]),
        as_calculation(calcs["calc_disc_cash"]),
        as_calculation(calcs["calc_pv"]),
        frame=ddf._meta,
    )

    registry_error = Registry(
        as_calculation(calcs["calc_v"]),
        as_calculation(calcs["calc_disc_factor"]),
        as_calculation(calcs["calc_pv"]),
        frame=ddf._meta,
    )

    return {"base": registry_base, "mode": registry_mode, "error": registry_error}


def test_model_template(ddf, registry):
    model_temp_base = ModelTemplate(registry["base"])
    model_temp_runtime = ModelTemplate(registry["mode"], runtime_settings=["mode"])
    model_temp_defined = ModelTemplate(registry["mode"], mode="M")

    # validate _frame
    assert_frame_equal(model_temp_base._frame, ddf._meta)
    assert_frame_equal(model_temp_runtime._frame, ddf._meta)
    assert_frame_equal(model_temp_defined._frame, ddf._meta)

    # test_runtime_settings
    assert model_temp_base.runtime_settings is None
    assert type(model_temp_runtime.runtime_settings["mode"]) is Setting
    assert model_temp_defined.runtime_settings is None

    # test_defined_settings

    # test_scenario

    # test_step

    # test_instructions

    # test_runtime_checks

    # test_dask_functions

    # test_dask_meta


# def test_model(df, ddf, registry, calcs):
#
#     m1_t = ModelTemplate(frame_meta=ddf._meta, registry=registry["base"])
#     m1_a = m1_t(frame=ddf)
#     m1_b = Model(frame=ddf, registry=registry["base"])
#
#     test1 = df.assign(
#         v=lambda x: calcs["calc_v"](x["i"]),
#         disc_factor=lambda x: calcs["calc_disc_factor"](x["v"]),
#         disc_cash=lambda x: calcs["calc_disc_cash"](x["cash"], x["disc_factor"]),
#         pv=lambda x: calcs["calc_pv"](x["disc_cash"]),
#     )
#
#     assert_frame_equal(m1_a.compute(), test1)
#     assert_frame_equal(m1_b.compute(), test1)
#
#     m2_t = ModelTemplate(
#         frame_meta=ddf._meta, registry=registry["mode"], runtime_settings=["mode"]
#     )
#     m2_a = m2_t(frame=ddf, mode="M")
#     m2_b = Model(frame=ddf, registry=registry["mode"], mode="M")
#
#     test2 = df.assign(
#         v=lambda x: calcs["calc_v_mode"](x["i"], mode="M"),
#         disc_factor=lambda x: calcs["calc_disc_factor"](x["v"]),
#         disc_cash=lambda x: calcs["calc_disc_cash"](x["cash"], x["disc_factor"]),
#         pv=lambda x: calcs["calc_pv"](x["disc_cash"]),
#     )
#
#     assert_frame_equal(m2_a.compute(), test2)
#     assert_frame_equal(m2_b.compute(), test2)
#
