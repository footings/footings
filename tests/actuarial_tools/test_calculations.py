import pandas as pd
from pandas.testing import assert_series_equal

from footings.actuarial_tools import (  # calculate_change_in_reserve,; calculate_interpolation,
    calculate_benefit_reserve,
    calculate_continuance,
    calculate_discount_factor,
    calculate_pv,
    calculate_pvfnb,
)


def test_calculate_continuance():
    mortality_rate = pd.Series([0.01, 0.015, 0.02])
    lapse_rate = pd.Series([0.2, 0.1, 0.05])
    lives_ed = calculate_continuance(mortality_rate, lapse_rate)
    assert_series_equal(lives_ed, ((1 - mortality_rate) * (1 - lapse_rate)).cumprod())
    lives_bd = lives_ed.shift(1, fill_value=1)
    lives_md = calculate_continuance(mortality_rate / 2, starting_duration=lives_bd)
    assert_series_equal(lives_md, lives_bd * (1 - mortality_rate / 2))


def test_calculate_discount_factor():
    interest_rate = pd.Series([0.03, 0.04, 0.05])
    v_ed = calculate_discount_factor(interest_rate)
    assert_series_equal(v_ed, pd.Series([0.970874, 0.933532, 0.889079]))
    v_md = calculate_discount_factor(interest_rate, period_adjustment=0.5)
    assert_series_equal(v_md, pd.Series([0.985329, 0.952020, 0.911034]))
    v_bd = calculate_discount_factor(interest_rate, period_adjustment=0)
    assert_series_equal(v_bd, pd.Series([1, 0.970874, 0.933532]))


def test_calculate_pv():
    assert_series_equal(calculate_pv(pd.Series([3, 2, 1])), pd.Series([6, 3, 1]))


def test_calculate_pvfnb():
    pvfb = pd.Series([6, 5, 3])
    pvfp = pd.Series([9, 6, 3])
    pvfnb = calculate_pvfnb(pvfb=pvfb, pvfp=pvfp, net_benefit_method="NLP")
    assert_series_equal(pvfnb, pd.Series([6.0, 4.0, 2.0]))


def test_calculate_benefit_reserve():
    pvfb = pd.Series([6, 5, 3])
    pvfp = pd.Series([9, 6, 3])
    pvfnb = calculate_pvfnb(pvfb=pvfb, pvfp=pvfp, net_benefit_method="NLP")
    lives = pd.Series([0.95, 0.9, 0.8])
    discount = pd.Series([0.95, 0.9, 0.85])
    br = calculate_benefit_reserve(
        pvfb=pvfb, pvfnb=pvfnb, lives=lives, discount=discount,
    )
    assert_series_equal(br, pd.Series([1.108033, 1.234568, 0.0]))
