import functools
import operator
from typing import Optional, Union

import pandas as pd


def calculate_continuance(
    *decrements: pd.Series, starting_duration: Optional[pd.Series] = None,
) -> pd.Series:
    """Apply a set of decrements together to form a continuance table.

    Parameters
    ----------
    *decrements : pd.Series
        The series of decrements to apply to calculate continuance.
    starting_duration : pd.Series, optional
        The start duration continuance value.

    Returns
    -------
    pd.Series
        The continuance table.

    Examples
    --------
    >>> import pandas as pd
    >>> from footings.actuarial_tools import calculate_continuance
    >>> mortality_rate = pd.Series([0.01, 0.015, 0.02])
    >>> lapse_rate = pd.Series([0.2, 0.1, 0.05])

    Assume -
    - mortality is applied uniformly over a duration
    - lapse occurs only at end of duration

    Calculate continuance at the end of a duration.
    >>> lives_ed = calculate_continuance(mortality_rate, lapse_rate)
    >>> lives_ed
    0   0.792000
    1   0.702108
    2   0.653663

    Shift one place to get the beginning duration value.
    >>> lives_bd = lives_ed.shift(1, fill_value=1)
    >>> lives_bd
    0   1.000000
    1   0.792000
    2   0.702108

    To get the value for the mid-point of the duration set a starting_duration value.
    >>> lives_md = calculate_continuance(mortality_rate / 2, starting_duration=lives_bd)
    >>> lives_md
    0   0.995000
    1   0.786060
    2   0.695087
    """
    if starting_duration is None:
        return functools.reduce(operator.mul, [1 - d for d in decrements]).cumprod()
    return starting_duration * functools.reduce(operator.mul, [1 - d for d in decrements])


def calculate_discount_factor(
    interest_rate: pd.Series, *, period_adjustment: Optional[Union[int, float]] = None
) -> pd.Series:
    """Calculate the discount factor over a series.

    Parameters
    ----------
    interest_rate : pd.Series
        A series of interest rate values
    period_adjustment : Union[int, float], optional
        An optinal adjustment to consider timing. As an example, use 0.5 for a midpoint.

    Returns
    -------
    pd.Series
        The discount factor.

    Examples
    --------
    >>> import pandas as pd
    >>> from footings.actuarial_tools import calculate_discount_factor
    >>> interest_rate = pd.Series([0.03, 0.04, 0.05])
    >>> v_ed = calculate_discount_factor(interest_rate)
    >>> v_ed
    0    0.970874
    1    0.933532
    2    0.889079

    >>> v_md = calculate_discount_factor(interest_rate, period_adjustment=0.5)
    >>> v_md
    0    0.985329
    1    0.952020
    2    0.911034

    >>> v_bd = calculate_discount_factor(interest_rate, period_adjustment=0)
    >>> v_bd
    0    1.000000
    1    0.970874
    2    0.933532
    """
    cum = (1 + interest_rate).cumprod()
    if period_adjustment is not None:
        cum = cum.shift(1, fill_value=1) * (1 + interest_rate) ** period_adjustment
    return 1 / cum


# def calculate_interpolation(
#     start: pd.Series,
#     end: pd.Series,
#     start_wt: Optional[pd.Series] = None,
#     end_wt: Optional[pd.Series] = None,
#     method: str = "linear",
# ):
#     """ """
#     return


def calculate_pv(series: pd.Series):
    """Calculate the present value of a series for each row.

    Parameters
    ----------
    series : pd.Series
        The series to calculate the present value from.

    Returns
    -------
    pd.Series
        The series with the present value for each row.

    Examples
    --------
    >>> import pandas as pd
    >>> from footings.actuarial_tools import calculate_pv
    >>> x = calculate_pv(pd.Series([3, 2, 1]))
    0    6
    1    3
    2    1
    """
    return series[::-1].cumsum()[::-1]


def calculate_pvfnb(pvfb: pd.Series, pvfp: pd.Series, net_benefit_method: str):
    """Calculate present value of future net benefits.

    Parameters
    ----------
    pvfb : pd.Series
        A series representing the present value of future benefits.
    pvfp : pd.Series
        A series representing the present value of premium future premium.
    net_benefit_method : str
        The net benefit method to use. Set to  -
        - NLP for net level premium
        - PT1 for 1 year preliminary term
        - PT2 for 2 year preliminary term

    Returns
    -------
    pd.Series
        A series representing the present value of net future benefits.

    Examples
    --------
    >>> import pandas as pd
    >>> from footings.actuarial_tools import calculate_pvfnb
    >>> pvfb = pd.Series([6, 5, 3])
    >>> pvfp = pd.Series([9, 6, 3])
    >>> pvfnb = calculate_pvfnb(pvfb=pvfb, pvfp=pvfp, net_benefit_method="NLP")
    >>> pvfnb
    0    6.0
    1    4.0
    2    2.0
    """
    if net_benefit_method == "NLP":
        nlp = pvfb.iat[0] / pvfp.iat[0]
        pvfnb = pvfp * nlp
    elif net_benefit_method == "PT1":
        nlp = pvfb.iat[1] / pvfb.iat[1]
        pvfnb = pvfp * nlp
        pvfnb.iat[0] = pvfb.iat[0]
    elif net_benefit_method == "PT2":
        nlp = pvfb.iat[2] = pvfb.iat[2]
        pvfnb = pvfp * nlp
        pvfnb.iat[0] = pvfb.iat[0]
        pvfnb.iat[1] = pvfb.iat[1]
    else:
        msg = f"The net_benefit_method of [{net_benefit_method}] is not recognized."
        msg += " Use one of NLP, PT1, or PT2."
        raise ValueError(msg)
    return pvfnb


def calculate_benefit_reserve(
    pvfb: pd.Series, pvfnb: pd.Series, lives: pd.Series, discount: pd.Series
):
    """Calculate benefit reserve.

    Parameters
    ----------
    pvfb : pd.Series
        A series representing the present value of future benefits.
    pvfnb : pd.Series
        A series representing the present value of future net benefits.
    lives : pd.Series
        A series representing the expected lives inforce.
    discount : pd.Series
        A series representing the discount factor.

    Returns
    -------
    pd.Series
        The benefit reserve over time.

    Examples
    --------
    >>> import pandas as pd
    >>> from footings.actuarial_tools import calculate_pvfnb, calculate_benefit_reserve
    >>> pvfb = pd.Series([6, 5, 3])
    >>> pvfp = pd.Series([9, 6, 3])
    >>> pvfnb = calculate_pvfnb(pvfb=pvfb, pvfp=pvfp, net_benefit_method="NLP")
    >>> lives = pd.Series([0.95, 0.9, 0.8])
    >>> discount = pd.Series([0.95, 0.9, 0.85])
    >>> calculate_benefit_reserve(
    ...     pvfb=pvfb,
    ...     pvfnb=pvfnb,
    ...     lives=lives,
    ...     discount=discount,
    ... )
    0    1.108033
    1    1.234568
    2    0.000000
    """
    return (pvfb - pvfnb).shift(-1, fill_value=0) / lives / discount


# def calculate_change_in_reserve(
#     reserve: pd.Series, lives: pd.Series, discount: pd.Series
# ):
#     """Calculate change in reserve."""
#     discount_shift = discount.shift(1, fill_value=1)
#     current_period = reserve * lives / (discount_shift / discount)
#     prior_period = reserve.shift(1, fill_value=0) * lives.shift(1, fill_value=1)
#     return current_period - prior_period


# def calculate_policy_year_benefit_reserve(
#     gross_premiums: pd.Series,
#     benefits: pd.Series,
#     lives: pd.Series,
#     discount: pd.Series,
#     net_benefit_method: str,
# ):
#     """Calculate a policy year benefit reserve.
#
#     Parameters
#     ----------
#     gross_premiums : pd.Series
#         The projected stream  of gross premiums.
#     benefits : pd.Series
#         The projected stream of benefits.
#     lives : pd.Series
#         The projected lives inforce at each duration.
#     discount : pd.Series
#         The discount to apply.
#     net_benefit_method : str
#         The net benefit method to use. Options are -
#             - NLP (Net Level Premium)
#             - PT1 (1 Year Preliminary Term)
#             - PT2 (2 Year Preliminary Term)
#
#     Returns
#     -------
#     pd.Series
#         A series with the benefit reserves.
#     """
#     pvfb = calculate_pv(benefits * lives * discount)
#     pvp = calculate_pv(gross_premiums * lives * discount)
#     pvfnb = calculate_pvfnb(pvfb, pvp, net_benefit_method)
#     benefit_reserve = (pvfb - pvfnb) / discount
#     return benefit_reserve
