import pytest
import numpy as np
import pandas as pd
import dask.dataframe as dd

from footings import Column, CReturn, Frame, FReturn, Setting, Registry, as_calculation


@pytest.fixture(scope="module")
def registry():

    #####################################################################################
    #  set up ddf
    #####################################################################################
    df = pd.DataFrame(
        {"t": [0, 1, 2, 3], "i": [0, 0.1, 0.09, 0.08], "cash": [1000, -350, -350, -350]}
    )
    df.set_index("t", inplace=True)
    ddf = dd.from_pandas(df, npartitions=1)

    #####################################################################################
    #  set up calcs
    #####################################################################################
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
        return np.cumprod(v)

    def calc_disc_cash(
        cash: Column("float"), disc_factor: Column("float")
    ) -> CReturn({"disc_cash": "float"}):
        return cash * disc_factor

    def calc_pv(disc_cash: Column("float")) -> CReturn({"pv": "float"}):
        return np.cumsum(disc_cash)

    #####################################################################################
    #  set up registries
    #####################################################################################

    registry_base = Registry(
        as_calculation(calc_v),
        as_calculation(calc_disc_factor),
        as_calculation(calc_disc_cash),
        as_calculation(calc_pv),
        starting_frame_meta=ddf._meta,
    )

    registry_repositioned = Registry(
        as_calculation(calc_disc_factor),
        as_calculation(calc_v),
        as_calculation(calc_disc_cash),
        as_calculation(calc_pv),
        starting_frame_meta=ddf._meta,
    )

    registry_mode = Registry()
    registry_mode.register(
        as_calculation(calc_v_mode),
        as_calculation(calc_disc_factor),
        as_calculation(calc_disc_cash),
        as_calculation(calc_pv),
        starting_frame_meta=ddf._meta,
    )

    registry_error = Registry(
        as_calculation(calc_v),
        as_calculation(calc_disc_factor),
        as_calculation(calc_pv),
        starting_frame_meta=ddf._meta,
    )

    return {
        "base": registry_base,
        "repositioned": registry_repositioned,
        "mode": registry_mode,
        "error": registry_error,
    }


def test_registered(registry):
    assert all(
        i in ["t", "i", "cash", "v", "disc_factor", "disc_cash", "pv"]
        for i in registry["base"].list_registered()
    )

    assert all(
        i in ["t", "i", "cash", "mode", "v", "disc_factor", "disc_cash", "pv"]
        for i in registry["mode"].list_registered()
    )


def test_predecesors(registry):
    assert all(
        i in ["cash", "disc_factor"] for i in registry["base"].predecesors("disc_cash")
    )


def test_ancestors(registry):
    assert all(
        i in ["cash", "i", "v", "disc_factor"]
        for i in registry["base"].ancestors("disc_cash")
    )


def test_successors(registry):
    assert all(i in ["disc_cash"] for i in registry["base"].successors("disc_factor"))


def test_descendants(registry):
    assert all(
        i in ["disc_cash", "pv"] for i in registry["base"].descendants("disc_factor")
    )


def test_get_settings(registry):
    pass


def test_get_functions(registry):
    pass


def test_get_calculations(registry):
    pass


def test_get_deterministic_assumptions(registry):
    pass


def test_get_stochastic_assumptions(registry):
    pass


def test_valid(registry):
    pass
