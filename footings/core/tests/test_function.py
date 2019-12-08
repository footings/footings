import pytest

from footings import Column, CReturn, Frame, FReturn, Setting
from footings.core.function import (
    BaseFunction,
    Assumption,
    as_assumption,
    Calculation,
    as_calculation,
)


def testBase_function():
    # using (Column) -> CReturn
    def func1(i: Column("float")) -> CReturn({"v": "float"}):
        return 1 / (1 + i)

    base1 = BaseFunction(func1, method="A")
    assert isinstance(base1, BaseFunction)

    # using (Column, Setting) -> CReturn
    def func2(
        i: Column("float"), period: Setting(allowed=["A", "M"], default="A")
    ) -> CReturn({"v": "float"}):
        if period == "A":
            return 1 / (1 + i)
        elif period == "M":
            return 1 / (1 + i / 12)

    base2 = BaseFunction(func2, method="A")
    assert isinstance(base2, BaseFunction)

    # using (Frame) -> FReturn
    def func3(df: Frame({"i": "float"})) -> FReturn({"v": "float"}):
        df["v"] = 1 / (1 + df["i"])
        return df

    base3 = BaseFunction(func3, method="A")
    assert isinstance(base3, BaseFunction)

    # using (Frame, Setting) -> FReturn
    def func4(
        df: Frame({"i": "float"}), period: Setting(allowed=["A", "M"], default="A")
    ) -> FReturn({"v": "float"}):
        if period == "A":
            df["v"] = 1 / (1 + df["i"])
            return df
        elif period == "M":
            df["v"] = 1 / (1 + df["i"] / 12)
            return df

    base4 = BaseFunction(func4, method="A")
    assert isinstance(base4, BaseFunction)


def test_calculation():
    def calc_v(i: Column("float")) -> CReturn({"v": "float"}):
        return 1 / (1 + i)

    v = Calculation(calc_v, method="A")
    assert isinstance(v, Calculation)
    assert v(1) == 0.5


def test_as_calculation():
    @as_calculation(method="A")
    def calc_v(i: Column("float")) -> CReturn({"v": "float"}):
        return 1 / (1 + i)

    assert isinstance(calc_v, Calculation)
    assert calc_v(1) == 0.5


def test_assumption():
    def get_i(df: Frame({"t": "int"})) -> FReturn({"i": "float"}):
        asn = pd.DataFrame({"t": [0, 1, 2, 3], "i": [0, 0.1, 0.09, 0.08]})
        return df.merge(asn, on="t")

    i = Assumption(get_i, method="A")
    assert isinstance(i, Assumption)
    test_df = pd.DataFrame({"t": [0, 1, 2, 3], "cash": [1000, -350, -350, -350]})
    assert_frame_equal(test_df.assign(i=[0, 0.1, 0.09, 0.08]), i(test_df))


def test_as_assumption():
    @as_assumption(method="A")
    def get_i(df: Frame({"t": "int"})) -> FReturn({"i": "float"}):
        asn = pd.DataFrame({"t": [0, 1, 2, 3], "i": [0, 0.1, 0.09, 0.08]})
        return df.merge(asn, on="t")

    assert isinstance(get_i, Assumption)
    test_df = pd.DataFrame({"t": [0, 1, 2, 3], "cash": [1000, -350, -350, -350]})
    assert_frame_equal(test_df.assign(i=[0, 0.1, 0.09, 0.08]), get_i(test_df))
