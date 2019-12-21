import numpy as np
import pandas as pd


def expand_frame_by_dates(
    df, freq, col_nm="date", start_dt="start_dt", end_dt="end_dt", weekday=None, **kwargs
):
    """Expands every row of a DataFrame to have a row for every date specified by the
    date range created by the combination of a start date, end date and frequency of
    date intervals.

    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe that needs to be expanded
    freq : str or DateOffset
        The date frequency of the expanding column
    col_nm : str
        The return name of the date column that is created
    start_dt : str
        The column name of the start date
    end_dt : str
        The column name of the end date
    weekday : int
        Optional weekday (applies only if freq is W)
    **kwargs :
        calendar_year : str
            Include column for calendar year with specified name
        calendar_quarter : str
            Include column for calendar quarter with specified name
        calendar_month : str
            Include column for calendar month with specified name
        calendar_week : str
            Include column for calendar week with specified name
        calendar_day : str
            Include column for calendar day with specified name
        duration_year : str
            Include column for duration year with specified name
        duration_quarter : str
            Include column for duration quarter with specified name
        duration_month : str
            Include column for duration month with specified name
        duration_week : str
            Include column for duration week with specified name
        duration_day : str
            Include column for duration day with specified name

    Returns
    -------
    pandas.DataFrame
        A dataframe that is expanded by the start date to end date for each row with an \
        additional column specified as col_nm and any included **kwargs.

    Examples
    --------
    >>> import pandas as pd
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "start_dt": ["2015-11-01", "2016-01-01"],
                "end_dt": ["2016-01-15", "2016-03-01"]
            }
        )
        df["start_dt"] = pd.to_datetime(df["start_dt"])
        df["end_dt"] = pd.to_datetime(df["end_dt"])

    >>> print(df)
       id   start_dt     end_dt
    0   1 2015-11-01 2016-01-15
    1   2 2016-01-01 2016-03-01

    >>> expand_frame_by_dates(df, freq="M")
       id   start_dt     end_dt       date
    0   1 2015-11-01 2016-01-15 2015-11-30
    1   1 2015-11-01 2016-01-15 2015-12-31
    2   1 2015-11-01 2016-01-15 2016-01-31
    3   2 2016-01-01 2016-03-01 2016-01-31
    4   2 2016-01-01 2016-03-01 2016-02-29
    5   2 2016-01-01 2016-03-01 2016-03-31

    >>> expand_frame_by_dates(df, freq="M", calendar_year="cal_yr")
       id   start_dt     end_dt       date  cal_yr
    0   1 2015-11-01 2016-01-15 2015-11-30    2015
    1   1 2015-11-01 2016-01-15 2015-12-31    2015
    2   1 2015-11-01 2016-01-15 2016-01-31    2016
    3   2 2016-01-01 2016-03-01 2016-01-31    2016
    4   2 2016-01-01 2016-03-01 2016-02-29    2016
    5   2 2016-01-01 2016-03-01 2016-03-31    2016

    >>> expand_frame_by_dates(df, freq="M", duration_month="dur_mn")
       id   start_dt     end_dt       date  dur_mn
    0   1 2015-11-01 2016-01-15 2015-11-30       1
    1   1 2015-11-01 2016-01-15 2015-12-31       2
    2   1 2015-11-01 2016-01-15 2016-01-31       3
    3   2 2016-01-01 2016-03-01 2016-01-31       1
    4   2 2016-01-01 2016-03-01 2016-02-29       2
    5   2 2016-01-01 2016-03-01 2016-03-31       3

    """

    assert col_nm not in df.columns
    assert freq in ["Y", "Q", "M", "W", "D"]

    if freq == "Y":
        offset = pd.offsets.YearEnd(0)
    elif freq == "Q":
        offset = pd.offsets.QuarterEnd(0)
    elif freq == "M":
        offset = pd.offsets.MonthEnd(0)
    elif freq == "W":
        offset = pd.offsets.Week(0, weekday=weekday)
    elif freq == "D":
        offset = pd.offsets.Day(0)

    list_dts = [
        pd.date_range(start=row[start_dt], end=row[end_dt] + offset, freq=freq)
        for i, row in df.iterrows()
    ]

    lens = [len(x) for x in list_dts]
    d = {c: np.repeat(df[c].values, lens) for c in df.columns}
    z = {col_nm: np.concatenate(list_dts)}

    df = pd.DataFrame({**d, **z})

    if "calendar_year" in kwargs:
        df[kwargs["calendar_year"]] = df[col_nm].dt.year

    if "calendar_quarter" in kwargs:
        df[kwargs["calendar_quarter"]] = df[col_nm].dt.quarter

    if "calendar_month" in kwargs:
        df[kwargs["calendar_month"]] = df[col_nm].dt.month

    if "calendar_week" in kwargs:
        df[kwargs["calendar_week"]] = df[col_nm].dt.week

    if "calendar_day" in kwargs:
        df[kwargs["calendar_day"]] = df[col_nm].dt.day

    if "duration_year" in kwargs:
        df[kwargs["duration_year"]] = (
            np.floor((df[col_nm] - df[start_dt]) / np.timedelta64(1, "Y")) + 1
        ).astype(int)

    if "duration_quarter" in kwargs:
        df[kwargs["duration_quarter"]] = (
            np.floor((df[col_nm] - df[start_dt]) / (3 * np.timedelta64(1, "M"))) + 1
        ).astype(int)

    if "duration_month" in kwargs:
        df[kwargs["duration_month"]] = (
            np.floor((df[col_nm] - df[start_dt]) / np.timedelta64(1, "M")) + 1
        ).astype(int)

    if "duration_week" in kwargs:
        df[kwargs["duration_week"]] = (
            np.floor((df[col_nm] - df[start_dt]) / np.timedelta64(1, "W")) + 1
        ).astype(int)

    if "duration_day" in kwargs:
        df[kwargs["duration_day"]] = (
            np.floor((df[col_nm] - df[start_dt]) / np.timedelta64(1, "D")) + 1
        ).astype(int)

    return df
