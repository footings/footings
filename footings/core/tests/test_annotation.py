import pytest
import pandas as pd

from footings import Column, CReturn, Frame, FReturn, Setting
from footings.core.annotation import _allowed_dtypes
from footings.core.function import parse_annotation


class TestvalidAssignedAnnotations:
    def test_assigned_column(self):
        Column("int")
        pytest.raises(AssertionError, Column, "x")

    def test_assigned_column_return(self):
        CReturn({"test": "int"})
        pytest.raises(AssertionError, CReturn, {"test": "x"})
        pytest.raises(AssertionError, CReturn, {"test1": "int", "test2": "int"})

    def test_assigned_frame(self):
        Frame({"test": int})
        Frame({"test1": "int", "test2": "int"})
        pytest.raises(AssertionError, Frame, {"test": "x"})
        pytest.raises(AssertionError, Frame, {"test1": "x", "test2": "int"})

    def test_assigned_frame_return(self):
        FReturn({"test": float})
        FReturn({"test1": float, "test2": "int"})
        pytest.raises(AssertionError, FReturn, {"test": "x"})
        pytest.raises(AssertionError, FReturn, {"test1": "x", "test2": "int"})

    def test_assigned_setting(self):
        s1 = Setting(allowed=["A", "M"])
        s2 = Setting(dtype=str, allowed=["A", "M"])
        pytest.raises(AssertionError, s1.valid, "z")
        pytest.raises(AssertionError, s2.valid, "z")


class TestvalidFunctionAnnotations:
    def test_valid_column_usage(self):
        # passes: valid combination Column -> CReturn
        def func1(i: Column("float")) -> CReturn({"v": "float"}):
            return 1 / (1 + i)

        parse_annotation(func1, method="A")

        # passes: valid combination (Column, Setting) -> CReturn
        def func2(
            i: Column("float"), period: Setting(allowed=["A", "M"], default="A")
        ) -> CReturn({"v": "float"}):
            if period == "A":
                return 1 / (1 + i)
            elif period == "M":
                return 1 / (1 + i / 12)

        parse_annotation(func2, method="A")

    def test_invalid_column_usage(self):
        # fails: Column not allowed as return annotation
        def func1(i: Column("float")) -> Column("float"):
            return 1 / (1 + i)

        pytest.raises(AssertionError, parse_annotation, function=func1, method="A")

        # fails: Column needs to be initalized
        def func2(i: Column) -> CReturn({"v": "float"}):
            return 1 / (1 + i)

        pytest.raises(AssertionError, parse_annotation, function=func2, method="A")

        # fails: CReturn needs to be initalized
        def func3(i: Column("float")) -> CReturn:
            return 1 / (1 + i)

        pytest.raises(AssertionError, parse_annotation, function=func3, method="A")

        # fails: CReturn not allowed as parameter annotation
        def func4(i: CReturn({"v": "float"})) -> CReturn({"v": "float"}):
            return 1 / (1 + i)

        pytest.raises(AssertionError, parse_annotation, function=func4, method="A")

    def test_valid_frame_usage(self):
        def func1(df: Frame({"i": "float"})) -> FReturn({"v": "float"}):
            df["v"] = 1 / (1 + df["i"])
            return df

        parse_annotation(func1, method="A")

        def func2(
            df: Frame({"i": "float"}), period: Setting(allowed=["A", "M"], default="A")
        ) -> FReturn({"v": "float"}):
            if period == "A":
                df["v"] = 1 / (1 + df["i"])
                return df
            elif period == "M":
                df["v"] = 1 / (1 + df["i"] / 12)
                return df

        parse_annotation(func1, method="A")

    def test_invalid_frame_usage(self):
        # fails: Frame not allowed as return annotation
        def func1(df: Frame({"i": "float"})) -> Frame({"v": "float"}):
            df["v"] = 1 / (1 + df["i"])
            return df

        pytest.raises(AssertionError, parse_annotation, function=func1, method="A")

        # fails: Frame needs to be initalized
        def func2(df: Frame) -> FReturn({"v": "float"}):
            df["v"] = 1 / (1 + df["i"])
            return df

        pytest.raises(AssertionError, parse_annotation, function=func2, method="A")

        # fails: FReturn needs to be initalized
        def func3(df: Frame({"i": "float"})) -> FReturn:
            df["v"] = 1 / (1 + df["i"])
            return df

        pytest.raises(AssertionError, parse_annotation, function=func3, method="A")

        # fails: FReturn not allowed as parameter annotation
        def func4(df: FReturn({"i": "float"})) -> FReturn({"v": "float"}):
            df["v"] = 1 / (1 + df["i"])
            return df

        pytest.raises(AssertionError, parse_annotation, function=func4, method="A")

    def test_invalid_combinations(self):
        # fails: invalid combination Frame -> CReturn
        def func1(df: Frame({"i": "float"})) -> CReturn({"v": "float"}):
            return None

        pytest.raises(AssertionError, parse_annotation, function=func1, method="A")

        # fails: invalid combination Column -> FReturn
        def func2(i: Column("float")) -> FReturn({"v": "float"}):
            return None

        pytest.raises(AssertionError, parse_annotation, function=func2, method="A")

        # fails: invalid combination (Column, Frame) -> FReturn
        def func3(
            i: Column("float"), df: Frame({"x": "float"})
        ) -> FReturn({"v": "float"}):
            return None

        pytest.raises(AssertionError, parse_annotation, function=func3, method="A")

        # fails: invalid combination (Column, Frame) -> CReturn
        def func4(
            i: Column("float"), df: Frame({"x": "float"})
        ) -> CReturn({"v": "float"}):
            return None

        pytest.raises(AssertionError, parse_annotation, function=func4, method="A")

        # fails: Setting not allowed as return annotation
        def func5(i: Column("float")) -> Setting(allowed=["A", "M"], default="A"):
            return None

        pytest.raises(AssertionError, parse_annotation, function=func5, method="A")


class TestvalidAllowedTypes:
    def test_allowed_dtypes(self):
        df = pd.DataFrame(
            {"x": [1, 2, 3], "dt": ["2018-12-31", "2019-12-31", "2020-12-31"]}
        )
        for x in _allowed_dtypes:
            if x != "datetime64":
                df.x.astype(x)
            else:
                df.dt.astype(x)


def test_parsing_annotations():
    def func_c_d(i, period):
        """Test of column functionality
        
        Parameters
        ----------
        i : Column("float")
            A column or vector of interest rates
            
        period : Setting(allowed=["A", "M"], default="A")
            A setting that modifies the interest rate to be a period 
            - A for Annual
            - M for Month

        Returns
        -------
        CReturn({"v": "float"})
            A column or vector of discount rates
        """
        if period == "A":
            return 1 / (1 + i)
        elif period == "M":
            return 1 / (1 + i / 12)

    def func_c_a(
        i: Column("float"), period: Setting(allowed=["A", "M"], default="A")
    ) -> CReturn({"v": "float"}):
        if period == "A":
            return 1 / (1 + i)
        elif period == "M":
            return 1 / (1 + i / 12)

    parse_annotation(func_c_d, method="D") == parse_annotation(func_c_a, method="A")

    def func_f_d(df, period):
        """Test of frame functionality
        
        Parameters
        ----------
        df : Frame({"i": "float"})
            A dataframe with an interest rate column
        period : Setting(allowed=["A", "M"], default="A")
            A setting that modifies the interest rate to be a period 
            - A for Annual
            - M for Month

        Returns
        -------
        FReturn({"v": "float"})
            The input dataframe with a column called v that represents the discount rate
        """
        if period == "A":
            df["v"] = 1 / (1 + df["i"])
            return df
        elif period == "M":
            df["v"] = 1 / (1 + df["i"] / 12)
            return df

    def func_f_a(
        df: Frame({"i": "float"}), period: Setting(allowed=["A", "M"], default="A")
    ) -> FReturn({"v": "float"}):
        if period == "A":
            df["v"] = 1 / (1 + df["i"])
            return df
        elif period == "M":
            df["v"] = 1 / (1 + df["i"] / 12)
            return df

    parse_annotation(func_f_d, method="D") == parse_annotation(func_f_a, method="A")
