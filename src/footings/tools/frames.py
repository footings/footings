import math

import pandas as pd

from footings import create_dispatch_function


def _month_diff(start, end):
    months = (end.year - start.year) * 12 + (end.month - start.month)
    if end.day > start.day:
        months += 1
    return months


def _day_diff(start, end):
    return (end - start).days


freq_dispatcher = create_dispatch_function("freq_dispatcher", parameters=("frequency",))


@freq_dispatcher.register(frequency="Y")
def _(start_dt: pd.Timestamp, end_dt: pd.Timestamp, col_date_nm: str):
    periods = math.ceil(_month_diff(start_dt, end_dt) / 12) + 1
    dates = pd.to_datetime(
        [start_dt + pd.DateOffset(years=period) for period in range(0, periods)]
    )
    return pd.DataFrame({col_date_nm: dates})


@freq_dispatcher.register(frequency="Q")
def _(start_dt: pd.Timestamp, end_dt: pd.Timestamp, col_date_nm: str):
    periods = math.ceil(_month_diff(start_dt, end_dt) / 3) + 1
    dates = pd.to_datetime(
        [start_dt + pd.DateOffset(months=period * 3) for period in range(0, periods)]
    )
    return pd.DataFrame({col_date_nm: dates})


@freq_dispatcher.register(frequency="M")
def _(start_dt: pd.Timestamp, end_dt: pd.Timestamp, col_date_nm: str):
    periods = _month_diff(start_dt, end_dt) + 1
    dates = pd.to_datetime(
        [start_dt + pd.DateOffset(months=period) for period in range(0, periods)]
    )
    return pd.DataFrame({col_date_nm: dates})


@freq_dispatcher.register(frequency="W")
def _(start_dt: pd.Timestamp, end_dt: pd.Timestamp, col_date_nm: str):
    periods = math.ceil(_day_diff(start_dt, end_dt) / 7) + 1
    dates = pd.to_datetime(
        [start_dt + pd.DateOffset(weeks=period) for period in range(0, periods)]
    )
    return pd.DataFrame({col_date_nm: dates})


@freq_dispatcher.register(frequency="D")
def _(start_dt: pd.Timestamp, end_dt: pd.Timestamp, col_date_nm: str):
    periods = _day_diff(start_dt, end_dt) + 1
    dates = pd.to_datetime(
        [start_dt + pd.DateOffset(days=period) for period in range(0, periods)]
    )
    return pd.DataFrame({col_date_nm: dates})


kwarg_dispatcher = create_dispatch_function("kwarg_dispatcher", parameters=("kw",))


@kwarg_dispatcher.register(kw="duration_year")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, start_dt: pd.Timestamp, **kwargs):
    tbl = kwarg_dispatcher(
        tbl, col_date_nm, col_nm, start_dt, kw="duration_month", **kwargs
    )
    tbl[col_nm] = tbl[col_nm].add(-1).floordiv(12).add(1)
    return tbl


@kwarg_dispatcher.register(kw="duration_quarter")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, start_dt: pd.Timestamp, **kwargs):
    tbl = kwarg_dispatcher(
        tbl, col_date_nm, col_nm, start_dt, kw="duration_month", **kwargs
    )
    tbl[col_nm] = tbl[col_nm].add(-1).floordiv(3).add(1)
    return tbl


@kwarg_dispatcher.register(kw="duration_month")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, start_dt: pd.Timestamp, **kwargs):
    tbl[col_nm] = (
        (tbl[col_date_nm].dt.year - start_dt.year) * 12
        + (tbl[col_date_nm].dt.month - start_dt.month)
        + 1
    ).astype("Int64")
    return tbl


@kwarg_dispatcher.register(kw="duration_week")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, start_dt: pd.Timestamp, **kwargs):
    tbl[col_nm] = (math.floor(_day_diff(tbl[col_date_nm], start_dt) / 7) + 1).astype(
        "Int64"
    )
    return tbl


@kwarg_dispatcher.register(kw="duration_day")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, start_dt: pd.Timestamp, **kwargs):
    tbl[col_nm] = (_day_diff(tbl[col_date_nm], start_dt)).astype("Int64")
    return tbl


@kwarg_dispatcher.register(kw="calendar_year")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, **kwargs):
    tbl[col_nm] = (tbl[col_date_nm].dt.year).astype("Int64")
    return tbl


@kwarg_dispatcher.register(kw="calendar_year_lag")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, **kwargs):
    tbl[col_nm] = (tbl[col_date_nm].shift(1).dt.year).astype("Int64")
    return tbl


@kwarg_dispatcher.register(kw="calendar_year_lead")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, **kwargs):
    tbl[col_nm] = (tbl[col_date_nm].shift(-1).dt.year).astype("Int64")
    return tbl


@kwarg_dispatcher.register(kw="calendar_quarter")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, **kwargs):
    tbl[col_nm] = (tbl[col_date_nm].dt.quarter).astype("Int64")
    return tbl


@kwarg_dispatcher.register(kw="calendar_quarter_lag")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, **kwargs):
    tbl[col_nm] = (tbl[col_date_nm].shift(1).dt.quarter).astype("Int64")
    return tbl


@kwarg_dispatcher.register(kw="calendar_quarter_lead")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, **kwargs):
    tbl[col_nm] = (tbl[col_date_nm].shift(-1).dt.quarter).astype("Int64")
    return tbl


@kwarg_dispatcher.register(kw="calendar_month")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, **kwargs):
    tbl[col_nm] = (tbl[col_date_nm].dt.month).astype("Int64")
    return tbl


@kwarg_dispatcher.register(kw="calendar_month_lag")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, **kwargs):
    tbl[col_nm] = (tbl[col_date_nm].shift(1).dt.month).astype("Int64")
    return tbl


@kwarg_dispatcher.register(kw="calendar_month_lead")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, **kwargs):
    tbl[col_nm] = (tbl[col_date_nm].shift(-1).dt.month).astype("Int64")
    return tbl


def create_frame(
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    frequency: str,
    col_date_nm: str,
    **kwargs,
) -> pd.DataFrame:
    """
    Create a frame with a date colum ranging from the start_dt to the end_dt.

    Additional columns can be created by passing kwargs (see list below) where the value is the
    assigned name. As an example, to add a column for duration month to be called DURATION_MONTH,
    passing the kwarg "duration_month": "DURATION_MONTH".

    Parameters
    ----------
    start_dt : pd.Timestamp
        The frame start date (first date row)
    end_dt : pd.Timestamp
        The frame end date (last date row).
    frequency : str
        The frequency at which records are created.
    col_date_nm : str
        The column name to assign the date column.
    kwargs :
        duration_year \n
        duration_quarter \n
        duration_month \n
        duration_week \n
        duration_day \n
        calendar_year \n
        calendar_year_lag \n
        calendar_year_lead \n
        calendar_quarter \n
        calendar_quarter_lag \n
        calendar_quarter_lead \n
        calendar_month \n
        calendar_month_lag \n
        calendar_month_lead

    Returns
    -------
    pandas.DataFrame
        A DataFrame with a date column and any passed kwargs.

    See Also
    --------
    footings.library.create_frame_from_record
    footings.library.expand_frame_per_record

    Examples
    --------
    >>> import pandas as pd
    >>> from footings.library import create_frame
    >>> frame = create_frame(
    >>>     start_dt = pd.Timestamp("2020-01-10"),
    >>>     end_dt = pd.Timestamp("2020-05-30"),
    >>>     frequency = "M",
    >>>     col_date_nm = "DATE",
    >>>     duration_month = "DURATION_MONTH"
    >>> )
    >>> frame
    >>> #       DATE	        DURATION_MONTH
    >>> # 0	2020-01-10	1
    >>> # 1	2020-02-10	2
    >>> # 2	2020-03-10	3
    >>> # 3	2020-04-10	4
    >>> # 4	2020-05-10	5
    >>> # 5	2020-06-10	6
    """
    tbl = freq_dispatcher(start_dt, end_dt, col_date_nm, frequency=frequency)
    for k, v in kwargs.items():
        tbl = kwarg_dispatcher(
            tbl=tbl, col_date_nm=col_date_nm, col_nm=v, start_dt=start_dt, kw=k
        )
    return tbl


def create_frame_from_record(
    record: pd.DataFrame,
    col_start_dt: str,
    col_end_dt: str,
    frequency: str,
    col_date_nm: str,
    **kwargs,
) -> pd.DataFrame:
    """
    Create a frame with a date colum ranging from the start_dt to the end_dt from a record.

    This function is to be used when you have a record of data from a DataFrame and you want to
    expand the data out over a date range. Note the DataFrame must contain a column for a start date
    and an end date. All columns from the original record will be duplicated the length of the date
    range. See example for usage.

    If wanting to do this for many records use the function ``footings.library.expand_frame_per_record``.
    When doing one record ``create_frame_from_record`` is faster than expand_frame_per_record.

    Parameters
    ----------
    record : pd.DataFrame
        A single record from a DataFrame.
    col_start_dt : str
        The name of the start date column.
    col_end_dt : str
        The name of the end date column.
    frequency : str
        The frequency at which records are created.
    col_date_nm : str
        The column name to assign the date column.
    kwargs :
        See kwargs under footings.library.create_frame

    Returns
    -------
    pandas.DataFrame
        A DataFrame with a date column, any passed kwargs, and columns from the record.

    Raises
    ------
    ValueError
        If the length of record is not 1.

    See Also
    --------
    footings.library.create_frame
    footings.library.expand_frame_per_record

    Examples
    --------
    >>> import pandas as pd
    >>> from footings.library import create_frame_from_record
    >>> record = pd.DataFrame(
    >>>     {
    >>>         "POLICY": ["P1"],
    >>>         "GENDER": ["M"],
    >>>         "START_DATE": [pd.Timestamp("2020-01-10")],
    >>>         "END_DATE": [pd.Timestamp("2020-05-30")]
    >>>     }
    >>> )
    >>> frame = create_frame_from_record(
    >>>     record=record,
    >>>     col_start_dt="START_DATE",
    >>>     col_end_dt="END_DATE",
    >>>     frequency="M",
    >>>     col_date_nm="DATE",
    >>>     duration_month="DURATION_MONTH"
    >>> )
    >>> frame
    >>> #       DATE            DURATION_MONTH	    POLICY      GENDER
    >>> # 0     2020-01-10	1	            P1	        M
    >>> # 1     2020-02-10	2	            P1	        M
    >>> # 2     2020-03-10	3	            P1	        M
    >>> # 3     2020-04-10	4	            P1	        M
    >>> # 4     2020-05-10	5	            P1	        M
    >>> # 5     2020-06-10	6	            P1	        M
    """
    record = record.to_dict(orient="record")
    if len(record) != 1:
        msg = f"The record must be a pd.DataFrame with one row. The record pass has {len(record)} rows."
        raise ValueError(msg)
    record = record[0]
    start_dt = record[col_start_dt]
    record.pop(col_start_dt)
    end_dt = record[col_end_dt]
    record.pop(col_end_dt)
    return create_frame(start_dt, end_dt, frequency, col_date_nm, **kwargs).assign(
        **record
    )


def expand_frame_per_record(
    frame: pd.DataFrame,
    col_start_dt: str,
    col_end_dt: str,
    frequency: str,
    col_date_nm: str,
    **kwargs,
) -> pd.DataFrame:
    """
    Create a frame with a date colum ranging from the start_dt to the end_dt from a record.

    This function expands the function ``footings.library.create_frame_from_record`` to cover the
    application of covering many records. Internally, it applies the function
    ``create_frame_from_record`` for each record.

    Parameters
    ----------
    frame : pd.DataFrame
        The DataFrame to expand.
    col_start_dt : str
        The name of the start date column.
    col_end_dt : str
        The name of the end date column.
    frequency : str
        The frequency at which records are created.
    col_date_nm : str
        The column name to assign the date column.
    kwargs :
        See kwargs under footings.library.create_frame

    Returns
    -------
    pandas.DataFrame
        A DataFrame with a date column, any passed kwargs, and columns from the original frame.

    See Also
    --------
    footings.library.create_frame
    footings.library.create_frame_from_record

    Examples
    --------
    >>> import pandas as pd
    >>> from footings.library import expand_frame_per_record
    >>> df = pd.DataFrame(
    >>>     {
    >>>         "POLICY": ["P1", "P2"],
    >>>         "GENDER": ["M", "F"],
    >>>         "START_DATE": [pd.Timestamp("2020-01-10"), pd.Timestamp("2020-02-01")],
    >>>         "END_DATE": [pd.Timestamp("2020-05-30"), pd.Timestamp("2020-04-18")]
    >>>     }
    >>> )
    >>> frame = expand_frame_per_record(
    >>>     frame=df,
    >>>     col_start_dt="START_DATE",
    >>>     col_end_dt="END_DATE",
    >>>     frequency="M",
    >>>     col_date_nm="DATE",
    >>>     duration_month="DURATION_MONTH"
    >>> )
    >>> frame
    >>> #       DATE            DURATION_MONTH  POLICY	    GENDER
    >>> # 0     2020-01-10	1	        P1	    M
    >>> # 1     2020-02-10	2	        P1	    M
    >>> # 2     2020-03-10	3	        P1	    M
    >>> # 3     2020-04-10	4	        P1	    M
    >>> # 4     2020-05-10	5	        P1	    M
    >>> # 5     2020-06-10	6	        P1	    M
    >>> # 6     2020-02-01	1	        P2	    F
    >>> # 7     2020-03-01	2	        P2	    F
    >>> # 8     2020-04-01	3	        P2	    F
    >>> # 9     2020-05-01	4	        P2	    F
    """
    return (
        frame.reset_index(drop=True)
        .groupby(level=0)
        .apply(
            lambda row: create_frame_from_record(
                record=row,
                col_start_dt=col_start_dt,
                col_end_dt=col_end_dt,
                frequency=frequency,
                col_date_nm=col_date_nm,
                **kwargs,
            ),
        )
        .reset_index(drop=True)
    )


def normalize_to_calendar_year(tbl, col_date_nm):
    """Normalize to calendar year."""
    raise NotImplementedError("Feature not implemented yet.")
