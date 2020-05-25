"""Test for audit.py"""

# pylint: disable=function-redefined, missing-function-docstring, too-many-locals

import os
import datetime

from attr import attrs, attrib
import pandas as pd
import xlsxwriter

from footings.to_xlsx import obj_to_xlsx, XlsxWorkbook

from .xlsx_helpers import compare_xlsx_files


def test_obj_to_xlsx(tmp_path):

    test_wb = os.path.join(tmp_path, "test-obj-to-xlsx.xlsx")
    wb = xlsxwriter.Workbook(test_wb)

    # test builtins
    wksht_builtins = wb.add_worksheet("test-builtins")
    obj_to_xlsx("test-bool", wksht_builtins, 1, 1, None)
    obj_to_xlsx(True, wksht_builtins, 1, 2, None)
    obj_to_xlsx("test-str", wksht_builtins, 2, 1, None)
    obj_to_xlsx("string", wksht_builtins, 2, 2, None)
    obj_to_xlsx("test-int", wksht_builtins, 3, 1, None)
    obj_to_xlsx(1, wksht_builtins, 3, 2, None)
    obj_to_xlsx("test-float", wksht_builtins, 4, 1, None)
    obj_to_xlsx(1.23456789, wksht_builtins, 4, 2, None)
    obj_to_xlsx("test-date", wksht_builtins, 5, 1, None)
    obj_to_xlsx(datetime.date(2018, 12, 31), wksht_builtins, 5, 2, None)
    obj_to_xlsx("test-datetime", wksht_builtins, 6, 1, None)
    obj_to_xlsx(datetime.datetime(2018, 12, 31, 1, 1), wksht_builtins, 6, 2, None)

    # test pd.Series
    wksht_series = wb.add_worksheet("test-series")
    obj_to_xlsx(pd.Series([1, 2, 3], name="numbers"), wksht_series, 1, 1, None)
    obj_to_xlsx(pd.Series(["a", "b", "c"], name="letters"), wksht_series, 7, 1, None)

    # test pd.DataFrame
    wksht_dataframe = wb.add_worksheet("test-dataframe")
    df = pd.DataFrame({"numbers": [1, 2], "letters": ["a", "b"]}, None)
    obj_to_xlsx(df, wksht_dataframe, 1, 1, None)

    # test mapping
    wksht_mapping = wb.add_worksheet("test-mapping")
    obj_to_xlsx({"key": "value"}, wksht_mapping, 1, 1, None)
    obj_to_xlsx({"key": [1, 2, 3]}, wksht_mapping, 4, 1, None)
    obj_to_xlsx({"key": df}, wksht_mapping, 9, 1, None)
    obj_to_xlsx({("tuple", "key"): 1}, wksht_mapping, 14, 1, None)

    # test iterable
    wksht_iterable = wb.add_worksheet("test-iterable")
    obj_to_xlsx([1, 2, 3], wksht_iterable, 1, 1, None)
    obj_to_xlsx([[1, 2, 3], [1, 2, 3]], wksht_iterable, 6, 1, None)
    obj_to_xlsx([df, df], wksht_iterable, 15, 1, None)

    # test custom
    @attrs
    class CustomOutput:
        """Custom Output"""

        a: int = attrib()
        b: int = attrib()

        def to_audit_format(self):
            return {"a": self.a, "b": self.b}

    custom1 = CustomOutput(1, 2)
    custom2 = CustomOutput(2, 4)

    wksht_custom = wb.add_worksheet("test-custom")
    obj_to_xlsx(custom1, wksht_custom, 1, 1, None)
    obj_to_xlsx(custom2, wksht_custom, 5, 1, None)

    # test callable
    def test_func(a, b):
        return a, b

    wksht_callable = wb.add_worksheet("test-function")
    obj_to_xlsx(test_func, wksht_callable, 1, 1, None)

    wb.close()

    expected_wb = os.path.join("tests", "data", "expected-obj-to-xlsx.xlsx")

    assert compare_xlsx_files(test_wb, expected_wb, [], {})


def test_xlsx_workbook(tmp_path):

    test_wb = os.path.join(tmp_path, "test-xlsx-workbook.xlsx")
    wb = XlsxWorkbook.create(test_wb)
    wb.add_worksheet("worksheet-1", start_row=1, start_col=1)
    wb.add_format("bold", bold=True)
    wb.write_obj("worksheet-1", "B2", xlsx_format="bold")
    wb.write_obj("worksheet-1", "C3", add_cols=1)
    wb.write_obj("worksheet-1", "B5", add_rows=1, xlsx_format="bold")
    wb.write_obj("worksheet-1", "C7", add_rows=1, add_cols=1)
    wb.close()

    expected_wb = os.path.join("tests", "data", "expected-xlsx-workbook.xlsx")

    assert compare_xlsx_files(test_wb, expected_wb, [], {})
