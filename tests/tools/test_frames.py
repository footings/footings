""""Test for frames.py"""

import os

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from footings.tools.frames import (
    freq_dispatcher,
    kwarg_dispatcher,
    create_frame,
    create_frame_from_record,
    expand_frame_per_record,
)

FILE = os.path.join("tests", "tools", "data", "frame-examples.xlsx")
TEST_PARAMS = [
    (
        "test-month-duration",
        {"usecols": "B,C", "skiprows": 3, "nrows": 7},
        {"usecols": "B:E", "skiprows": 15, "nrows": 30},
    ),
    (
        "test-month-calendar",
        {"usecols": "B,C", "skiprows": 3, "nrows": 13},
        {"usecols": "B:K", "skiprows": 21, "nrows": 29},
    ),
]
IDS = [param[0] for param in TEST_PARAMS]


def _get_kwargs_from_spreadsheet(sheet, **kwargs):
    columns = ["kwarg", "value"]
    df = pd.read_excel(
        FILE, sheet, header=None, names=columns, engine="openpyxl", **kwargs
    )
    return df.set_index(["kwarg"]).to_dict()["value"]


def _get_return_from_spreadsheet(sheet, **kwargs):
    df = pd.read_excel(FILE, sheet, engine="openpyxl", **kwargs)
    column_types = {
        col: "Int64"
        for col in df.columns
        if col.split("_")[0] in ["calendar", "duration"]
    }
    return df.astype(column_types)


@pytest.mark.parametrize("sheet, func_kwargs, output_kwargs", TEST_PARAMS, ids=IDS)
def test_create_frame(sheet, func_kwargs, output_kwargs):
    expected = _get_return_from_spreadsheet(sheet, **output_kwargs)

    kwargs = _get_kwargs_from_spreadsheet(sheet, **func_kwargs)

    # test frequency_dispatcher
    freq_kws = ["frequency", "start_dt", "end_dt", "col_date_nm"]
    freq_ret = freq_dispatcher(**{k: v for k, v in kwargs.items() if k in freq_kws})
    assert_frame_equal(freq_ret, expected[[kwargs["col_date_nm"]]])

    # test kwarg_dispatcher
    kws = [k for k in kwargs.keys() if k.split("_")[0] in ["calendar", "duration"]]
    other_kws = {k: v for k, v in kwargs.items() if k in ["col_date_nm", "start_dt"]}
    for kw in kws:
        ret = kwarg_dispatcher(kw=kw, tbl=freq_ret.copy(), col_nm=kw, **other_kws)
        assert_frame_equal(ret, expected[[kwargs["col_date_nm"], kw]])

    # test create_frame
    frame_ret = create_frame(**kwargs)
    assert_frame_equal(frame_ret, expected)


def test_create_frame_from_record():

    record = pd.DataFrame(
        {
            "POLICY": ["P1"],
            "GENDER": ["M"],
            "START_DATE": [pd.Timestamp("2020-01-10")],
            "END_DATE": [pd.Timestamp("2020-05-30")],
        }
    )
    test = create_frame_from_record(
        record=record,
        col_start_dt="START_DATE",
        col_end_dt="END_DATE",
        frequency="M",
        col_date_nm="DATE",
        duration_month="DURATION_MONTH",
    )
    dates = [
        "2020-01-10",
        "2020-02-10",
        "2020-03-10",
        "2020-04-10",
        "2020-05-10",
        "2020-06-10",
    ]
    expected = pd.DataFrame(
        {
            "DATE": [pd.Timestamp(x) for x in dates],
            "DURATION_MONTH": pd.Series([1, 2, 3, 4, 5, 6], dtype="Int64"),
            "POLICY": ["P1", "P1", "P1", "P1", "P1", "P1"],
            "GENDER": ["M", "M", "M", "M", "M", "M"],
        }
    )
    assert_frame_equal(test, expected)


def test_expand_frame_per_record():
    df = pd.DataFrame(
        {
            "POLICY": ["P1", "P2"],
            "GENDER": ["M", "F"],
            "START_DATE": [pd.Timestamp("2020-01-10"), pd.Timestamp("2020-02-01")],
            "END_DATE": [pd.Timestamp("2020-05-30"), pd.Timestamp("2020-04-18")],
        }
    )
    test = expand_frame_per_record(
        frame=df,
        col_start_dt="START_DATE",
        col_end_dt="END_DATE",
        frequency="M",
        col_date_nm="DATE",
        duration_month="DURATION_MONTH",
    )
    dates1 = [
        "2020-01-10",
        "2020-02-10",
        "2020-03-10",
        "2020-04-10",
        "2020-05-10",
        "2020-06-10",
    ]
    dates2 = ["2020-02-01", "2020-03-01", "2020-04-01", "2020-05-01"]
    expected = pd.DataFrame(
        {
            "DATE": [pd.Timestamp(x) for x in dates1 + dates2],
            "DURATION_MONTH": pd.Series([1, 2, 3, 4, 5, 6, 1, 2, 3, 4], dtype="Int64"),
            "POLICY": ["P1", "P1", "P1", "P1", "P1", "P1", "P2", "P2", "P2", "P2"],
            "GENDER": ["M", "M", "M", "M", "M", "M", "F", "F", "F", "F"],
        }
    )
    assert_frame_equal(test, expected)
