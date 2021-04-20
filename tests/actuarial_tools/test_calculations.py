import pandas as pd
import pytest
from pandas.testing import assert_series_equal

from footings.actuarial_tools import (  # calc_change_in_reserve,
    calc_benefit_reserve,
    calc_continuance,
    calc_discount,
    calc_interpolation,
    calc_pv,
    calc_pvfnb,
)


def test_calc_continuance():
    mortality_rate = pd.Series([0.01, 0.015, 0.02])
    lapse_rate = pd.Series([0.2, 0.1, 0.05])
    lives_ed = calc_continuance(mortality_rate, lapse_rate)
    assert_series_equal(lives_ed, ((1 - mortality_rate) * (1 - lapse_rate)).cumprod())
    lives_bd = lives_ed.shift(1, fill_value=1)
    lives_md = calc_continuance(mortality_rate / 2, starting_duration=lives_bd)
    assert_series_equal(lives_md, lives_bd * (1 - mortality_rate / 2))


def test_calc_discount():
    interest_rate = pd.Series([0.03, 0.04, 0.05])
    v_ed = calc_discount(interest_rate)
    assert_series_equal(v_ed, pd.Series([0.970874, 0.933532, 0.889079]))
    v_md = calc_discount(interest_rate, t_adj=0.5)
    assert_series_equal(v_md, pd.Series([0.985329, 0.952020, 0.911034]))
    v_bd = calc_discount(interest_rate, t_adj=0)
    assert_series_equal(v_bd, pd.Series([1, 0.970874, 0.933532]))


def test_calc_interpolation():
    # test nonzero values
    val_0 = pd.Series([1, 2, 3])
    val_1 = pd.Series([2, 3, 4])
    wt_0 = pd.Series([0.5, 0.5, 0.5])
    linear = calc_interpolation(val_0, val_1, wt_0, method="linear")
    assert_series_equal(linear, pd.Series([1.5, 2.5, 3.5]))
    log = calc_interpolation(val_0, val_1, wt_0, method="log-linear")
    assert_series_equal(log, pd.Series([1.414214, 2.449490, 3.464102]))

    # test one zero value
    val_0 = pd.Series([0, 1, 2])
    val_1 = pd.Series([1, 2, 3])
    wt_0 = pd.Series([0.5, 0.5, 0.5])
    linear = calc_interpolation(val_0, val_1, wt_0, method="linear")
    assert_series_equal(linear, pd.Series([0.5, 1.5, 2.5]))
    log = calc_interpolation(val_0, val_1, wt_0, method="log-linear")
    assert_series_equal(log, pd.Series([0.414214, 1.449490, 2.464102]))

    # test two zero values
    val_0 = pd.Series([0, 0, 1])
    val_1 = pd.Series([0, 1, 2])
    wt_0 = pd.Series([0.5, 0.5, 0.5])
    linear = calc_interpolation(val_0, val_1, wt_0, method="linear")
    assert_series_equal(linear, pd.Series([0, 0.5, 1.5]))
    log = calc_interpolation(val_0, val_1, wt_0, method="log-linear")
    assert_series_equal(log, pd.Series([0, 0.414214, 1.449490]))

    # test value less than zero
    val_0 = pd.Series([-1, 0, 1])
    val_1 = pd.Series([0, 1, 2])
    wt_0 = pd.Series([0.5, 0.5, 0.5])
    linear = calc_interpolation(val_0, val_1, wt_0, method="linear")
    assert_series_equal(linear, pd.Series([-0.5, 0.5, 1.5]))
    # log-linear raises ValueError
    pytest.raises(ValueError, calc_interpolation, val_0, val_1, wt_0, method="log-linear")


def test_calc_pv():
    assert_series_equal(calc_pv(pd.Series([3, 2, 1])), pd.Series([6, 3, 1]))


def test_calc_pvfnb():
    pvfb = pd.Series([6, 5, 3])
    pvfp = pd.Series([9, 6, 3])
    pvfnb = calc_pvfnb(pvfb=pvfb, pvfp=pvfp, net_benefit_method="NLP")
    assert_series_equal(pvfnb, pd.Series([6.0, 4.0, 2.0]))


def test_calc_benefit_reserve():
    pvfb = pd.Series([6, 5, 3])
    pvfp = pd.Series([9, 6, 3])
    pvfnb = calc_pvfnb(pvfb=pvfb, pvfp=pvfp, net_benefit_method="NLP")
    lives = pd.Series([0.95, 0.9, 0.8])
    discount = pd.Series([0.95, 0.9, 0.85])
    br = calc_benefit_reserve(pvfb=pvfb, pvfnb=pvfnb, lives=lives, discount=discount,)
    assert_series_equal(br, pd.Series([1.108033, 1.234568, 0.0]))
