import pandas as pd
from pandas.util.testing import assert_frame_equal
import unittest

from footings import (
    AssumptionDeterministic,
    as_assumption_deterministic,
    AssumptionStochastic,
    as_assumption_stochastic,
    Frame,
    FReturn,
)


def test_assumption_deterministic():
    def get_i(df: Frame({"t": "int"})) -> FReturn({"i": "float"}):
        asn = pd.DataFrame({"t": [0, 1, 2, 3], "i": [0, 0.1, 0.09, 0.08]})
        return df.merge(asn, on="t")

    i = AssumptionDeterministic(get_i, method="A")
    assert isinstance(i, AssumptionDeterministic)
    test_df = pd.DataFrame({"t": [0, 1, 2, 3], "cash": [1000, -350, -350, -350]})
    assert_frame_equal(test_df.assign(i=[0, 0.1, 0.09, 0.08]), i(test_df))


def test_as_assumption_deterministic():
    @as_assumption_deterministic(method="A")
    def get_i(df: Frame({"t": "int"})) -> FReturn({"i": "float"}):
        asn = pd.DataFrame({"t": [0, 1, 2, 3], "i": [0, 0.1, 0.09, 0.08]})
        return df.merge(asn, on="t")

    assert isinstance(get_i, AssumptionDeterministic)
    test_df = pd.DataFrame({"t": [0, 1, 2, 3], "cash": [1000, -350, -350, -350]})
    assert_frame_equal(test_df.assign(i=[0, 0.1, 0.09, 0.08]), get_i(test_df))


def test_assumption_stochastic():
    def get_i(df: Frame({"t": "int"})) -> FReturn({"i": "float"}):
        asn = pd.DataFrame({"t": [0, 1, 2, 3], "i": [0, 0.1, 0.09, 0.08]})
        return df.merge(asn, on="t")

    i = AssumptionStochastic(get_i, method="A")
    assert isinstance(i, AssumptionStochastic)
    test_df = pd.DataFrame({"t": [0, 1, 2, 3], "cash": [1000, -350, -350, -350]})
    assert_frame_equal(test_df.assign(i=[0, 0.1, 0.09, 0.08]), i(test_df))


def test_as_assumption_stochastic():
    @as_assumption_stochastic(method="A")
    def get_i(df: Frame({"t": "int"})) -> FReturn({"i": "float"}):
        asn = pd.DataFrame({"t": [0, 1, 2, 3], "i": [0, 0.1, 0.09, 0.08]})
        return df.merge(asn, on="t")

    assert isinstance(get_i, AssumptionStochastic)
    test_df = pd.DataFrame({"t": [0, 1, 2, 3], "cash": [1000, -350, -350, -350]})
    assert_frame_equal(test_df.assign(i=[0, 0.1, 0.09, 0.08]), get_i(test_df))
