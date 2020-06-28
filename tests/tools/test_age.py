from datetime import date

import pandas as pd
from pandas.testing import assert_series_equal

from footings.tools import calculate_age


def test_calculate_age():

    birth_dt = date(1990, 5, 30)
    as_of_dt = date(2020, 4, 6)
    ts_birth_dt = pd.Timestamp("1990-05-30")
    ts_as_of_dt = pd.Timestamp("2020-04-06")
    pd_birth_dt = pd.Series([date(1990, 5, 30)], dtype="datetime64[ns]")
    pd_as_of_dt = pd.Series([date(2020, 4, 6)], dtype="datetime64[ns]")

    # pd.Series, pd.Series
    assert_series_equal(
        calculate_age(pd_birth_dt, pd_as_of_dt, "ALB"), pd.Series([29], dtype="int32")
    )
    assert_series_equal(
        calculate_age(pd_birth_dt, pd_as_of_dt, "ANB"), pd.Series([30], dtype="int32")
    )
    assert_series_equal(
        calculate_age(pd_birth_dt, pd_as_of_dt, "ACB"), pd.Series([30], dtype="int32")
    )

    # pd.Series, date
    assert_series_equal(
        calculate_age(pd_birth_dt, as_of_dt, "ALB"), pd.Series([29], dtype="int32")
    )
    assert_series_equal(
        calculate_age(pd_birth_dt, as_of_dt, "ANB"), pd.Series([30], dtype="int32")
    )
    assert_series_equal(
        calculate_age(pd_birth_dt, as_of_dt, "ACB"), pd.Series([30], dtype="int32")
    )

    # pd.Series, pd.Timestamp
    assert_series_equal(
        calculate_age(pd_birth_dt, ts_as_of_dt, "ALB"), pd.Series([29], dtype="int32")
    )
    assert_series_equal(
        calculate_age(pd_birth_dt, ts_as_of_dt, "ANB"), pd.Series([30], dtype="int32")
    )
    assert_series_equal(
        calculate_age(pd_birth_dt, ts_as_of_dt, "ACB"), pd.Series([30], dtype="int32")
    )

    # date, date
    assert calculate_age(birth_dt, as_of_dt, "ALB") == 29
    assert calculate_age(birth_dt, as_of_dt, "ANB") == 30
    assert calculate_age(birth_dt, as_of_dt, "ACB") == 30

    # date, pd.Series
    assert_series_equal(
        calculate_age(birth_dt, pd_as_of_dt, "ALB"), pd.Series([29], dtype="int32")
    )
    assert_series_equal(
        calculate_age(birth_dt, pd_as_of_dt, "ANB"), pd.Series([30], dtype="int32")
    )
    assert_series_equal(
        calculate_age(birth_dt, pd_as_of_dt, "ACB"), pd.Series([30], dtype="int32")
    )

    # date, pd.Timestamp
    assert calculate_age(birth_dt, ts_as_of_dt, "ALB") == 29
    assert calculate_age(birth_dt, ts_as_of_dt, "ANB") == 30
    assert calculate_age(birth_dt, ts_as_of_dt, "ACB") == 30

    # pd.Timestamp, pd.Timestamp
    assert calculate_age(ts_birth_dt, ts_as_of_dt, "ALB") == 29
    assert calculate_age(ts_birth_dt, ts_as_of_dt, "ANB") == 30
    assert calculate_age(ts_birth_dt, ts_as_of_dt, "ACB") == 30

    # pd.Timestamp, pd.Series
    assert_series_equal(
        calculate_age(ts_birth_dt, pd_as_of_dt, "ALB"), pd.Series([29], dtype="int32")
    )
    assert_series_equal(
        calculate_age(ts_birth_dt, pd_as_of_dt, "ANB"), pd.Series([30], dtype="int32")
    )
    assert_series_equal(
        calculate_age(ts_birth_dt, pd_as_of_dt, "ACB"), pd.Series([30], dtype="int32")
    )

    # pd.Timestamp, date
    assert calculate_age(ts_birth_dt, as_of_dt, "ALB") == 29
    assert calculate_age(ts_birth_dt, as_of_dt, "ANB") == 30
    assert calculate_age(ts_birth_dt, as_of_dt, "ACB") == 30
