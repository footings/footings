from pandas.util.testing import assert_frame_equal
import numpy as np
import pandas as pd
import pyarrow as pa
import dask.dataframe as dd
import unittest
import pytest
from collections import namedtuple

from footings import (
    Column,
    CReturn,
    Setting,
    Calculation,
    as_calculation,
    Schema,
    Model,
    ModelContainer,
)

from footings.core.utils import _generate_message


@pytest.fixture(scope="module")
def schema():
    base = pa.schema([pa.field("A", pa.int64()), pa.field("B", pa.int8())])

    Schemas = namedtuple("Schemas", "base")
    return Schemas(base=base)


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
        i: Column("float"), mode: Setting(dtype=str, allowed=["A", "M"])
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

    Calcs = namedtuple("Calcs", "v v_mode disc_factor disc_cash pv")

    return Calcs(
        v=calc_v,
        v_mode=calc_v_mode,
        disc_factor=calc_disc_factor,
        disc_cash=calc_disc_cash,
        pv=calc_pv,
    )


def test_model(ddf, calcs):
    pass
