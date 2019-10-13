import pytest
import pandas as pd
from pandas.util.testing import assert_frame_equal
import unittest

from footings import Column, CReturn, Frame, FReturn, Setting
from footings.core.function import (
    _BaseFunction,
    func_annotation_valid,
    _get_column_inputs,
    _get_column_ouputs,
    _get_setting_inputs,
)


class TestValidateAnnotations:
    def test_valid_column_usage(self):
        # passes: valid combination Column -> CReturn
        def func1(i: Column(float)) -> CReturn({"v": float}):
            return 1 / (1 + i)

        assert func_annotation_valid(func1) is True

        # passes: valid combination (Column, Setting) -> CReturn
        def func2(
            i: Column(float), period: Setting(allowed=["A", "M"], default="A")
        ) -> CReturn({"v": float}):
            if period == "A":
                return 1 / (1 + i)
            elif period == "M":
                return 1 / (1 + i / 12)

        assert func_annotation_valid(func1) is True

    def test_invalid_column_usage(self):
        # fails: Column not allowed as return annotation
        def func1(i: Column(float)) -> Column(float):
            return 1 / (1 + i)

        pytest.raises(AssertionError, func_annotation_valid, func1)

        # fails: Column needs to be initalized
        def func2(i: Column) -> CReturn({"v": float}):
            return 1 / (1 + i)

        pytest.raises(AssertionError, func_annotation_valid, func2)

        # fails: CReturn needs to be initalized
        def func3(i: Column(float)) -> CReturn:
            return 1 / (1 + i)

        pytest.raises(AssertionError, func_annotation_valid, func3)

        # fails: CReturn not allowed as parameter annotation
        def func4(i: CReturn({"v": float})) -> CReturn({"v": float}):
            return 1 / (1 + i)

        pytest.raises(AssertionError, func_annotation_valid, func4)

    def test_valid_frame_usage(self):
        def func1(df: Frame({"i": float})) -> FReturn({"v": float}):
            df["v"] = 1 / (1 + df["i"])
            return df

        assert func_annotation_valid(func1) is True

        def func2(
            df: Frame({"i": float}), period: Setting(allowed=["A", "M"], default="A")
        ) -> FReturn({"v": float}):
            if period == "A":
                df["v"] = 1 / (1 + df["i"])
                return df
            elif period == "M":
                df["v"] = 1 / (1 + df["i"] / 12)
                return df

        assert func_annotation_valid(func1) is True

    def test_invalid_frame_usage(self):
        # fails: Frame not allowed as return annotation
        def func1(df: Frame({"i": float})) -> Frame({"v": float}):
            df["v"] = 1 / (1 + df["i"])
            return df

        pytest.raises(AssertionError, func_annotation_valid, func1)

        # fails: Frame needs to be initalized
        def func2(df: Frame) -> FReturn({"v": float}):
            df["v"] = 1 / (1 + df["i"])
            return df

        pytest.raises(AssertionError, func_annotation_valid, func2)

        # fails: FReturn needs to be initalized
        def func3(df: Frame({"i": float})) -> FReturn:
            df["v"] = 1 / (1 + df["i"])
            return df

        pytest.raises(AssertionError, func_annotation_valid, func3)

        # fails: FReturn not allowed as parameter annotation
        def func4(df: FReturn({"i": float})) -> FReturn({"v": float}):
            df["v"] = 1 / (1 + df["i"])
            return df

        pytest.raises(AssertionError, func_annotation_valid, func4)

    def test_invalid_combinations(self):
        # fails: invalid combination Frame -> CReturn
        def func1(df: Frame({"i": float})) -> CReturn({"v": float}):
            return None

        pytest.raises(AssertionError, func_annotation_valid, func1)

        # fails: invalid combination Column -> FReturn
        def func2(i: Column(float)) -> FReturn({"v": float}):
            return None

        pytest.raises(AssertionError, func_annotation_valid, func2)

        # fails: invalid combination (Column, Frame) -> FReturn
        def func3(i: Column(float), df: Frame({"x": float})) -> FReturn({"v": float}):
            return None

        pytest.raises(AssertionError, func_annotation_valid, func3)

        # fails: invalid combination (Column, Frame) -> CReturn
        def func4(i: Column(float), df: Frame({"x": float})) -> CReturn({"v": float}):
            return None

        pytest.raises(AssertionError, func_annotation_valid, func4)

        # fails: Setting not allowed as return annotation
        def func5(i: Column(float)) -> Setting(allowed=["A", "M"], default="A"):
            return None

        pytest.raises(AssertionError, func_annotation_valid, func5)


class Test_BaseFunction:
    def test_base_function(self):
        # using (Column) -> CReturn
        def func1(i: Column(float)) -> CReturn({"v": float}):
            return 1 / (1 + i)

        base1 = _BaseFunction(func1)
        assert isinstance(base1, _BaseFunction)

        # using (Column, Setting) -> CReturn
        def func2(
            i: Column(float), period: Setting(allowed=["A", "M"], default="A")
        ) -> CReturn({"v": float}):
            if period == "A":
                return 1 / (1 + i)
            elif period == "M":
                return 1 / (1 + i / 12)

        base2 = _BaseFunction(func2)
        assert isinstance(base2, _BaseFunction)

        # using (Frame) -> FReturn
        def func3(df: Frame({"i": float})) -> FReturn({"v": float}):
            df["v"] = 1 / (1 + df["i"])
            return df

        assert func_annotation_valid(func1) is True
        base3 = _BaseFunction(func3)
        assert isinstance(base3, _BaseFunction)

        # using (Frame, Setting) -> FReturn
        def func4(
            df: Frame({"i": float}), period: Setting(allowed=["A", "M"], default="A")
        ) -> FReturn({"v": float}):
            if period == "A":
                df["v"] = 1 / (1 + df["i"])
                return df
            elif period == "M":
                df["v"] = 1 / (1 + df["i"] / 12)
                return df

        base4 = _BaseFunction(func4)
        assert isinstance(base4, _BaseFunction)
