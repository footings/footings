import pytest
import pandas as pd

from footings.function_library import expand_frame_by_dates


@pytest.fixture
def frame():
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "start_dt": ["2015-11-01", "2016-01-01"],
            "end_dt": ["2016-01-15", "2016-03-01"],
        }
    )
    df["start_dt"] = pd.to_datetime(df["start_dt"])
    df["end_dt"] = pd.to_datetime(df["end_dt"])
    return df


def test_expand_year(frame):
    # test base
    df1 = expand_frame_by_dates(frame, freq="Y")
    df2 = expand_frame_by_dates(frame, freq="Y", calendar_year="cal_yr")
    df3 = expand_frame_by_dates(frame, freq="Y", duration_year="dur_yr")
    assert type(df1) is pd.DataFrame

    # test calendar

    # test duration
    pass


def test_expand_quarter():
    pass


def test_expand_month():
    pass


def test_expand_week():
    pass


def test_expand_day():
    pass
