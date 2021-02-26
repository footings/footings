import math

import numpy as np
import pandas as pd

from footings.utils import dispatch_function


def _month_diff(start, end):
    months = (end.year - start.year) * 12 + (end.month - start.month)
    if end.day > start.day:
        months += 1
    return months


def _day_diff(start, end):
    return (end - start).days


@dispatch_function(key_parameters=("frequency",))
def freq_dispatcher(
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    col_date_nm: str,
    frequency: str,
    end_duration: str = None,
):
    msg = "No registered function based on passed paramters and no default function."
    raise NotImplementedError(msg)


@freq_dispatcher.register(frequency="Y")
def _(
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    col_date_nm: str,
    end_duration: str = None,
):
    periods = math.ceil(_month_diff(start_dt, end_dt) / 12) + 1
    frame = pd.DataFrame()
    frame[col_date_nm] = pd.to_datetime(
        [start_dt + pd.DateOffset(years=period) for period in range(0, periods)]
    )
    if end_duration is not None:
        frame[end_duration] = pd.to_datetime(
            [start_dt + pd.DateOffset(years=period) for period in range(1, periods + 1)]
        )
    return frame


@freq_dispatcher.register(frequency="Q")
def _(
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    col_date_nm: str,
    end_duration: str = None,
):
    periods = math.ceil(_month_diff(start_dt, end_dt) / 3) + 1
    frame = pd.DataFrame()
    frame[col_date_nm] = pd.to_datetime(
        [start_dt + pd.DateOffset(months=period * 3) for period in range(0, periods)]
    )
    if end_duration is not None:
        frame[end_duration] = pd.to_datetime(
            [
                start_dt + pd.DateOffset(months=period * 3)
                for period in range(1, periods + 1)
            ]
        )
    return frame


@freq_dispatcher.register(frequency="M")
def _(
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    col_date_nm: str,
    end_duration: str = None,
):
    periods = _month_diff(start_dt, end_dt) + 1
    frame = pd.DataFrame()
    frame[col_date_nm] = pd.to_datetime(
        [start_dt + pd.DateOffset(months=period) for period in range(0, periods)]
    )
    if end_duration is not None:
        frame[end_duration] = pd.to_datetime(
            [start_dt + pd.DateOffset(months=period) for period in range(1, periods + 1)]
        )
    return frame


@freq_dispatcher.register(frequency="W")
def _(
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    col_date_nm: str,
    end_duration: str = None,
):
    periods = math.ceil(_day_diff(start_dt, end_dt) / 7) + 1
    frame = pd.DataFrame()
    frame[col_date_nm] = pd.to_datetime(
        [start_dt + pd.DateOffset(weeks=period) for period in range(0, periods)]
    )
    if end_duration is not None:
        frame[end_duration] = pd.to_datetime(
            [start_dt + pd.DateOffset(weeks=period) for period in range(1, periods + 1)]
        )
    return frame


@freq_dispatcher.register(frequency="D")
def _(
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    col_date_nm: str,
    end_duration: str = None,
):
    periods = _day_diff(start_dt, end_dt) + 1
    frame = pd.DataFrame()
    frame[col_date_nm] = pd.to_datetime(
        [start_dt + pd.DateOffset(days=period) for period in range(0, periods)]
    )
    if end_duration is not None:
        frame[end_duration] = pd.to_datetime(
            [start_dt + pd.DateOffset(days=period) for period in range(1, periods + 1)]
        )
    return frame


@dispatch_function(key_parameters=("kw",))
def kwarg_dispatcher(
    tbl: pd.DataFrame,
    col_date_nm: str,
    col_nm: str,
    start_dt: pd.Timestamp,
    kw: str,
    **kwargs,
):
    msg = "No registered function based on passed paramters and no default function."
    raise NotImplementedError(msg)


@kwarg_dispatcher.register(kw="duration_year")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, start_dt: pd.Timestamp, **kwargs):
    tbl = kwarg_dispatcher(
        tbl=tbl,
        col_date_nm=col_date_nm,
        col_nm=col_nm,
        start_dt=start_dt,
        kw="duration_month",
        **kwargs,
    )
    tbl[col_nm] = tbl[col_nm].add(-1).floordiv(12).add(1)
    return tbl


@kwarg_dispatcher.register(kw="duration_quarter")
def _(tbl: pd.DataFrame, col_date_nm: str, col_nm: str, start_dt: pd.Timestamp, **kwargs):
    tbl = kwarg_dispatcher(
        tbl=tbl,
        col_date_nm=col_date_nm,
        col_nm=col_nm,
        start_dt=start_dt,
        kw="duration_month",
        **kwargs,
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
    """Create a frame with a date colum ranging from the start_dt to the end_dt.

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
        end_duration \n
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
    footings.actuarial_tools.create_frame_from_record
    footings.actuarial_tools.expand_frame_per_record

    Examples
    --------
    >>> import pandas as pd
    >>> from footings.actuarial_tools import create_frame
    >>> frame = create_frame(
    ...     start_dt = pd.Timestamp("2020-01-10"),
    ...     end_dt = pd.Timestamp("2020-05-30"),
    ...     frequency = "M",
    ...     col_date_nm = "DATE",
    ...     duration_month = "DURATION_MONTH"
    ... )
    >>> frame
              DATE  DURATION_MONTH
    0   2020-01-10               1
    1   2020-02-10               2
    2   2020-03-10               3
    3   2020-04-10               4
    4   2020-05-10               5
    5   2020-06-10               6
    """
    end_duration = kwargs.pop("end_duration", None)
    tbl = freq_dispatcher(
        start_dt=start_dt,
        end_dt=end_dt,
        col_date_nm=col_date_nm,
        frequency=frequency,
        end_duration=end_duration,
    )
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

    If wanting to do this for many records use the function ``footings.actuarial_tools.expand_frame_per_record``.
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
        See kwargs under footings.actuarial_tools.create_frame

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
    footings.actuarial_tools.create_frame
    footings.actuarial_tools.expand_frame_per_record

    Examples
    --------
    >>> import pandas as pd
    >>> from footings.actuarial_tools import create_frame_from_record
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
    record = record.to_dict(orient="records")
    if len(record) != 1:
        msg = f"The record must be a pd.DataFrame with one row. The record pass has {len(record)} rows."
        raise ValueError(msg)
    record = record[0]
    start_dt = record[col_start_dt]
    record.pop(col_start_dt)
    end_dt = record[col_end_dt]
    record.pop(col_end_dt)

    return create_frame(
        start_dt=start_dt,
        end_dt=end_dt,
        frequency=frequency,
        col_date_nm=col_date_nm,
        **kwargs,
    ).assign(**record)


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

    This function expands the function ``footings.actuarial_tools.create_frame_from_record`` to cover the
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
        See kwargs under footings.actuarial_tools.create_frame

    Returns
    -------
    pandas.DataFrame
        A DataFrame with a date column, any passed kwargs, and columns from the original frame.

    See Also
    --------
    footings.actuarial_tools.create_frame
    footings.actuarial_tools.create_frame_from_record

    Examples
    --------
    >>> import pandas as pd
    >>> from footings.actuarial_tools import expand_frame_per_record
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


def frame_add_exposure(
    frame: pd.DataFrame,
    *,
    begin_date: pd.Timestamp = None,
    end_date: pd.Timestamp = None,
    exposure_name: str = "EXPOSURE",
    begin_duration_col: str = "DATE_BD",
    end_duration_col: str = "DATE_ED",
):
    """Add an exposure values to a duration based frame.

    Parameters
    ----------
    frame : pd.DataFrame
        The frame to add an exposure value.
    begin_date : pd.Timestamp, optional
        Begin date for measuring exposure, by default None.
    end_date : pd.Timestamp, optional
        End date for measuring exposure, by default None.
    exposure_name : str, optional
        The name to give the column added representing the exposure, by default = EXPOSURE.
    begin_duration_col : str, optional
        The column name representing the start of a duration, by default DATE_BD.
    end_duration_col : str, optional
        The column name representing the end of a duration, by default DATE_ED.

    Raises
    ------
    ValueError
        If begin_duration_col is not in frame.
    ValueError
        if end_ducration_col is not in frame.

    Returns
    -------
    pd.DataFrame
        The frame with an additional column (name given to exposure) with the exposure value.

    Notes
    -----
    As an example, with a date of 2020-03-15 and a duration covering the period
    from 2020-03-10 through 2020-04-10, the exposure would be 0.838710 = 26 / 31.
    Notice the 15th is included as a day in the numerator and the end date is
    excluded in the denominator.

    Examples
    --------
    >>> import pandas as pd
    >>> from footings.actuarial_tools import frame_add_exposure
    >>>
    >>> frame = pd.DataFrame(
    >>>     {
    >>>         "BEGIN_DURATION_COL": pd.to_datetime(
    >>>             ["2020-02-10", "2020-03-10", "2020-04-10", "2020-05-10",]
    >>>         ),
    >>>         "END_DURATION_COL": pd.to_datetime(
    >>>             ["2020-03-10", "2020-04-10", "2020-05-10", "2020-06-10"]
    >>>         ),
    >>>     }
    >>> )
    >>>
    >>> example = frame_add_exposure(
    >>>     frame.copy(),
    >>>     exposure_name="EXPOSURE",
    >>>     begin_duration_col="BEGIN_DURATION_COL",
    >>>     end_duration_col="END_DURATION_COL",
    >>>     begin_date=pd.Timestamp("2020-03-15"),
    >>>     end_date=pd.Timestamp("2020-04-20"),
    >>> )
    >>>
    >>> example
    >>> #       BEGIN_DURATION_COL      END_DURATION_COL	EXPOSURE
    >>> # 0	2020-02-10	        2020-03-10	        0.000000
    >>> # 1	2020-03-10	        2020-04-10	        0.838710
    >>> # 2	2020-04-10	        2020-05-10	        0.838710
    >>> # 3	2020-05-10	        2020-06-10	        0.000000
    """
    if begin_duration_col not in frame.columns:
        raise ValueError(
            f"The begin_duration_col [{begin_duration_col}] is not in the frame."
        )
    if end_duration_col not in frame.columns:
        raise ValueError(
            f"The end_duration_col [{end_duration_col}] is not in the frame."
        )

    days_period = (frame[end_duration_col] - frame[begin_duration_col]).dt.days
    condlist, choicelist = [], []

    if begin_date is not None:
        begin_days = (frame[end_duration_col] - begin_date).dt.days.clip(lower=0)
        condlist.append(begin_days == 0)
        choicelist.append(0)
        condlist.append(begin_days <= days_period)
        choicelist.append((begin_days) / days_period)

    if end_date is not None:
        end_days = (frame[end_duration_col] - end_date).dt.days.clip(lower=0)
        condlist.append(end_days > 0)
        choicelist.append((1 - ((end_days - 1) / days_period)).clip(lower=0, upper=1))

    frame[exposure_name] = np.select(condlist, choicelist, default=1.0)
    return frame


def frame_add_weights(
    frame: pd.DataFrame,
    as_of_dt: pd.Timestamp,
    *,
    begin_duration_col: str = "DATE_BD",
    end_duration_col: str = "DATE_ED",
    wt_current_name: str = "WT_0",
    wt_next_name: str = "WT_1",
):
    """Add cell weights to a duration based frame as an as of date.

    Parameters
    ----------
    frame : pd.DataFrame
        The frame to add an exposure value.
    as_of_dt : pd.Timestamp
        The as of date to assign weights.
    begin_duration_col : str, optional
        The column name representing the start of a duration, by default DATE_BD.
    end_duration_col : str, optional
        The column name representing the end of a duration, by default DATE_ED.
    wt_currrent_name : str, optional
        The name of the column representing the current duration, by default WT_0.
    wt_next_name : str, optional
        The name of the column representing the next duration, by default WT_1.

    Raises
    ------
    ValueError
        If begin_duration_col is not in frame.
    ValueError
        If end_duration_col is not in frame.

    Returns
    -------
    pd.DataFrame
        The frame with an additional column (name given to exposure) with the exposure value.

    Notes
    -----
    As an example, with a date of 2020-02-15 and a duration covering the period
    from 2020-02-10 through 2020-03-10, the current period weight would be
    0.827586 = 24 / 29. Notice the 15th is included as a day in the numerator
    and tend end date is excluded as a day in the numerator. The next period
    weight would be 1 - 0.827586 or 0.172414.

    Examples
    --------
    >>> import pandas as pd
    >>> from footings.actuarial_tools import frame_add_weights
    >>>
    >>> frame = pd.DataFrame(
    >>>     {
    >>>         "BEGIN_DURATION_COL": pd.to_datetime(
    >>>             ["2020-02-10", "2020-03-10", "2020-04-10", "2020-05-10",]
    >>>         ),
    >>>         "END_DURATION_COL": pd.to_datetime(
    >>>             ["2020-03-10", "2020-04-10", "2020-05-10", "2020-06-10"]
    >>>         ),
    >>>     }
    >>> )
    >>>
    >>> example = frame_add_weights(
    >>>     frame.copy(),
    >>>     begin_duration_col="BEGIN_DURATION_COL",
    >>>     end_duration_col="END_DURATION_COL",
    >>>     as_of_dt=pd.Timestamp("2020-02-15"),
    >>>     wt_current_name="WT_0",
    >>>     wt_next_name="WT_1",
    >>> )
    >>>
    >>> example
    >>> #     	    BEGIN_DURATION_COL  END_DURATION_COL        WT_0        WT_1
    >>> # 0	    2020-02-10	        2020-03-10	        0.827586    0.172414
    >>> # 1	    2020-03-10	        2020-04-10	        0.827586    0.172414
    >>> # 2	    2020-04-10	        2020-05-10	        0.827586    0.172414
    >>> # 3	    2020-05-10	        2020-06-10	        0.827586    0.172414
    """
    if begin_duration_col not in frame.columns:
        raise ValueError(
            f"The begin_duration_col [{begin_duration_col}] is not in the frame."
        )
    if end_duration_col not in frame.columns:
        raise ValueError(
            f"The end_duration_col [{end_duration_col}] is not in the frame."
        )

    query = f"{begin_duration_col} <= @as_of_dt and {end_duration_col} >= @as_of_dt"
    src = frame[[begin_duration_col, end_duration_col]].query(query)
    dur_n_days = (src[end_duration_col].iat[0] - src[begin_duration_col].iat[0]).days
    wt_current = ((src[end_duration_col].iat[0] - as_of_dt).days) / dur_n_days
    frame[wt_current_name] = wt_current
    frame[wt_next_name] = 1 - wt_current
    return frame


def frame_filter(
    frame,
    *,
    begin_date: pd.Timestamp = None,
    end_date: pd.Timestamp = None,
    begin_duration_col: str = "DATE_BD",
    end_duration_col: str = "DATE_ED",
):
    """Filter a duration based frame based on a being and/or an end date.

    Parameters
    ----------
    frame : pd.DataFrame
        The frame to add an exposure value.
    begin_date : pd.Timestamp, optional
        Begin date for measuring exposure, by default None.
    end_date : pd.Timestamp, optional
        End date for measuring exposure, by default None.
    begin_duration_col : str, optional
        The column name representing the start of a duration, by default DATE_BD.
    end_duration_col : str, optional
        The column name representing the end of a duration, by default DATE_ED.

    Raises
    ------
    ValueError
        If begin_duration_col is not in frame.
    ValueError
        If end_duration_col is not in frame.

    Returns
    -------
    pd.DataFrame
        The frame with an additional column (name given to exposure) with the exposure value.

    Examples
    --------
    >>> import pandas as pd
    >>> from footings.actuarial_tools import frame_filter
    >>>
    >>> frame = pd.DataFrame(
    >>>     {
    >>>         "BEGIN_DURATION_COL": pd.to_datetime(
    >>>             ["2020-02-10", "2020-03-10", "2020-04-10", "2020-05-10",]
    >>>         ),
    >>>         "END_DURATION_COL": pd.to_datetime(
    >>>             ["2020-03-10", "2020-04-10", "2020-05-10", "2020-06-10"]
    >>>         ),
    >>>     }
    >>> )
    >>> frame
    >>> #   BEGIN_DURATION_COL  END_DURATION_COL
    >>> # 0 2020-02-10          2020-03-10
    >>> # 1 2020-03-10          2020-04-10
    >>> # 2 2020-04-10          2020-05-10
    >>> # 3 2020-05-10          2020-06-10
    >>>
    >>> example = frame_filter(
    >>>     frame,
    >>>     begin_date=pd.Timestamp("2020-03-15"),
    >>>     end_date=pd.Timestamp("2020-05-01"),
    >>>     begin_duration_col="BEGIN_DURATION_COL",
    >>>     end_duration_col="END_DURATION_COL",
    >>> )
    >>> example
    >>> #   BEGIN_DURATION_COL  END_DURATION_COL
    >>> # 1 2020-03-10          2020-04-10
    >>> # 2 2020-04-10          2020-05-10
    """
    if begin_duration_col not in frame.columns:
        raise ValueError(
            f"The begin_duration_col [{begin_duration_col}] is not in the frame."
        )
    if end_duration_col not in frame.columns:
        raise ValueError(
            f"The end_duration_col [{end_duration_col}] is not in the frame."
        )
    if begin_date is not None and end_date is None:
        query = f"{end_duration_col} >= @begin_date"
    elif begin_date is None and end_date is not None:
        query = f"{begin_duration_col} <= @end_date"
    elif begin_date is not None and end_date is not None:
        query = f"{end_duration_col} >= @begin_date and {begin_duration_col} <= @end_date"
    else:
        query = None

    if query is not None:
        return frame.query(query)
    return frame
