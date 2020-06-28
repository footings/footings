from datetime import date
from typing import Union

import pandas as pd

from footings import create_dispatch_function

_PARAMS = ("type_birth_dt", "type_as_of_dt")
_calculate_age = create_dispatch_function("calculate_age", parameters=_PARAMS)


def calculate_age(
    birth_dt: Union[date, pd.Timestamp, pd.Series],
    as_of_dt: Union[date, pd.Timestamp, pd.Series],
    method: str,
) -> Union[int, pd.Series]:
    """
    Calculate age given a birth date and an as of date.

    The calculate age accepts many different types for both the birth_dt and as_of_dt. Thus, this
    formula can be used to calculate the age difference between a single date as well as a pandas
    series of dates.

    Parameters
    ----------
    birth_dt : date or pd.Timestamp or pd.Series
        The birth date.
    as_of_dt : date or pd.Timestamp or pd.Series
        The as of date to calculate age.
    method : str
        Options are - \n
        - ALB = Age last birthday \n
        - ACB = Age closest birthday \n
        - ANB = Age next birthday \n

    Returns
    -------
    int or pd.Series
        Returns int if both birth_dt and as_of_dt are not pd.Series. Returns a pd.Series if any
        parameter is a pd.Series.

    Examples
    --------
    >>> birth_dt = date(1990, 5, 30)
    >>> as_of_dt = date(2020, 4, 6)
    >>> ts_birth_dt = pd.Timestamp("1990-05-30")
    >>> ts_as_of_dt = pd.Timestamp("2020-04-06")
    >>> pd_birth_dt = pd.Series([date(1990, 5, 30)], dtype="datetime64[ns]")
    >>> pd_as_of_dt = pd.Series([date(2020, 4, 6)], dtype="datetime64[ns]")
    >>>
    >>> calculate_age(pd_birth_dt, pd_as_of_dt, "ALB") # pd.Series([29])
    >>> calculate_age(pd_birth_dt, as_of_dt, "ANB") # pd.Series([30])
    >>> calculate_age(pd_birth_dt, ts_as_of_dt, "ACB") # pd.Series([30])
    >>>
    >>> calculate_age(birth_dt, as_of_dt, "ALB") # 29
    >>> calculate_age(birth_dt, pd_as_of_dt, "ANB") # pd.Series([30])
    >>> calculate_age(birth_dt, ts_as_of_dt, "ACB") # 30
    >>>
    >>> calculate_age(ts_birth_dt, ts_as_of_dt, "ALB") # 29
    >>> calculate_age(ts_birth_dt, pd_as_of_dt, "ANB") # pd.Series([30])
    >>> calculate_age(ts_birth_dt, as_of_dt, "ACB") # 30
    """
    return _calculate_age(
        type_birth_dt=type(birth_dt),
        type_as_of_dt=type(as_of_dt),
        birth_dt=birth_dt,
        as_of_dt=as_of_dt,
        method=method,
    )


def _series_calculate_age(birth_dt, as_of_dt, method):
    diff = (as_of_dt.year - birth_dt.year) * 12 + (as_of_dt.month - birth_dt.month)
    if method == "ALB":
        return diff.floordiv(12).astype("int32")
    if method == "ANB":
        return diff.floordiv(12).add(1).astype("int32")
    if method == "ACB":
        return diff.div(12).round(0).astype("int32")
    raise ValueError(f"The method passed [{method}] is not known. See the documentation.")


def _base_calculate_age(birth_dt, as_of_dt, method):
    diff = (as_of_dt.year - birth_dt.year) * 12 + (as_of_dt.month - birth_dt.month)
    if method == "ALB":
        return diff // 12
    if method == "ANB":
        return diff // 12 + 1
    if method == "ACB":
        return int(round(diff / 12, 0))
    raise ValueError(f"The method passed [{method}] is not known. See the documentation.")


@_calculate_age.register(type_birth_dt=pd.Series, type_as_of_dt=pd.Series)
def _(birth_dt, as_of_dt, method):
    return _series_calculate_age(birth_dt.dt, as_of_dt.dt, method)


@_calculate_age.register(type_birth_dt=pd.Series, type_as_of_dt=date)
def _(birth_dt, as_of_dt, method):
    return _series_calculate_age(birth_dt.dt, as_of_dt, method)


@_calculate_age.register(type_birth_dt=pd.Series, type_as_of_dt=pd.Timestamp)
def _(birth_dt, as_of_dt, method):
    return _series_calculate_age(birth_dt.dt, as_of_dt, method)


@_calculate_age.register(type_birth_dt=date, type_as_of_dt=date)
def _(birth_dt, as_of_dt, method):
    return _base_calculate_age(birth_dt, as_of_dt, method)


@_calculate_age.register(type_birth_dt=date, type_as_of_dt=pd.Series)
def _(birth_dt, as_of_dt, method):
    return _series_calculate_age(birth_dt, as_of_dt.dt, method)


@_calculate_age.register(type_birth_dt=date, type_as_of_dt=pd.Timestamp)
def _(birth_dt, as_of_dt, method):
    return _base_calculate_age(birth_dt, as_of_dt, method)


@_calculate_age.register(type_birth_dt=pd.Timestamp, type_as_of_dt=pd.Timestamp)
def _(birth_dt, as_of_dt, method):
    return _base_calculate_age(birth_dt, as_of_dt, method)


@_calculate_age.register(type_birth_dt=pd.Timestamp, type_as_of_dt=pd.Series)
def _(birth_dt, as_of_dt, method):
    return _series_calculate_age(birth_dt, as_of_dt.dt, method)


@_calculate_age.register(type_birth_dt=pd.Timestamp, type_as_of_dt=date)
def _(birth_dt, as_of_dt, method):
    return _base_calculate_age(birth_dt, as_of_dt, method)
