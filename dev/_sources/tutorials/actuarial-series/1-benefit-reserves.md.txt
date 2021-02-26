---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
kernelspec:
  display_name: Footings
  language: python
  name: footings

execution:
  timeout: -1
---

# Benefit Reserves

## Introduction

This is a the first tutorial in the [Actuarial Series](index.md) describing how to use the [Footings framework](https://www.github.com/footings/footings) to perform common actuarial functions.  This tutorial will demonstrate how to calculate a benefit reserve.

A benefit reserve is necessary whenever the expected benefit stream differs porportionally over time relative to the expected premium stream. That is, if premiums are constant for a policy holder and their expected cost go up overtime, part of the premiums paid early in the life of the policy must be set aside to pay for higher expected claims later in the life of the policy.

## Starting Data

For this example, we will assume we are starting with the following data.

```{code-cell} ipython3
import pandas as pd

df = pd.read_csv("benefit-reserve-example.csv")
df
```

Do note that each row in the table above represents a year. Thus, the timing of cashflows and when decrements are applied can vary even within a row. For this example, assume -

- premium is earned at the begining of each duration,
- benefits are paid in the middle of each duration,
- mortality is spread uniformly through each duration, and
- lapses happen at the end of each duration.


## Calculating a Benefit Reserve

As mentioned in the intro, a benefit reserve is needed any time there is mismatch in the level of expected premiums relative to the expected benefits to be paid. When calculating a benefit reserve, the following items need to be present -

- expected premiums (given in table),
- expected benefits (given in table),
- probability of the policy holder reaching each policy duration, and
- the time value-of-money.

The Footings framework has a function `calc_continuance` that can be used to calculate the probability of the policy holder each each policy duration. The code of this is shown below alongside the provided table.

```{code-cell} ipython3
from footings.actuarial_tools import calc_continuance

lives_ed = calc_continuance(df["mortality_rate"], df["lapse_rate"])
lives_bd = lives_ed.shift(1, fill_value=1)
lives_md = calc_continuance(df["mortality_rate"] / 2, starting_duration=lives_bd)

df["l_bd"] = lives_bd
df["l_md"] = lives_md
df["l_ed"] = lives_ed

df[["policy_duration", "mortality_rate", "lapse_rate", "l_bd", "l_md", "l_ed"]]
```

A few items to note -

- the lives at the end of the duration is the cumulative product of (1 - mortality_rate) x (1 - lapse_rate),
- the lives at the beginning of each duration is equal to the lives at the end of each duration for the next policy year (year 1 beginning duration is set to 1), and
- the lives in the middle of each duration is equal to lives_bd x (1 - mortality_rate / 2).

Next, the discount factors are calculated to consider the time value-of-money. The code for this is below.

```{code-cell} ipython3
from footings.actuarial_tools import calc_discount

df["v_bd"] = calc_discount(df["interest_rate"], t_adj=0)
df["v_md"] = calc_discount(df["interest_rate"], t_adj=0.5)
df["v_ed"] = calc_discount(df["interest_rate"])

df[["policy_duration", "interest_rate", "v_bd", "v_md", "v_ed"]]
```

To calculate a benefit reserve, the present value of future benefits and future gross premiums need to be known. The code for this is below. As the code shows, both benefits and premiums are multiplied by the probability of a life receiving/paying the benefit/premium times the respective discount factor.

```{code-cell} ipython3
from footings.actuarial_tools import calc_pv

df["pvfb"] = calc_pv(df["benefits"] * df["l_md"] * df["v_md"])
df["pvgp"] = calc_pv(df["gross_premium"] * df["l_bd"] * df["v_bd"])

df[["policy_duration", "benefits", "l_md", "v_md", "pvfb", "gross_premium", "l_bd", "v_bd", "pvgp"]]
```

With the known present value of both benefits and premiums, the present value of future net benefits can be calculated. This function calculates the net level premium (NLP) which is the time 0 present value of benefits ($1,172.38) divided by the present value of gross premiums (\$1,953.97). The NLP is then multiplied by the present value of premium. This column represent the time 0 value of future premium that needs to be available to pay future benefits.

```{code-cell} ipython3
from footings.actuarial_tools import calc_pvfnb

df["pvfnb"] = calc_pvfnb(
    pvfb=df["pvfb"],
    pvfp=df["pvgp"],
    net_benefit_method="NLP"
)

df[["policy_duration", "pvfb", "pvgp", "pvfnb"]]
```

With both the present value of future benefits and the present value of future net benefits known, the benefit reserve is calculated using the formula `calc_benefit_reserve`. This formula is equal to pvfb minus the pvfnb shifted one row. Given both pvfb and pvfnb are time 0 values (i.e., both multipled by lives and the discount factor), to get the appropriate ending duration value it is divided by both the lives and discount factor.

```{code-cell} ipython3
from footings.actuarial_tools import calc_benefit_reserve

df["benefit_reserve"] = calc_benefit_reserve(
    pvfb=df["pvfb"],
    pvfnb=df["pvfnb"],
    lives=df["l_ed"],
    discount=df["v_ed"]
)

df[["policy_duration", "gross_premium", "benefits", "l_ed", "v_ed", "pvfb", "pvfnb", "benefit_reserve"]]
```

To verify the formula works correctly, it can be viewed from a financial perspective. The code for this is below. The values for each duration are at the beginning of the duration.

```{code-cell} ipython3
def change_in_reserve(reserve: pd.Series, lives: pd.Series, discount: pd.Series):
    """Calculate change in reserve."""
    discount_shift = discount.shift(1, fill_value=1)
    current_period = reserve * lives / (discount_shift / discount)
    prior_period = reserve.shift(1, fill_value=0) * lives.shift(1, fill_value=1)
    return current_period - prior_period

df2 = pd.DataFrame()
df2["income"] = df["gross_premium"] * df["l_bd"]
df2["benefits"] = df["benefits"] * df["l_md"] * (df["v_md"] / df["v_bd"])
df2["benefit_reserve"] = df["benefit_reserve"]
df2["change_in_reserve"] = change_in_reserve(
    df["benefit_reserve"], df["l_ed"], df["v_ed"]
)
df2["net_income"] = df2["income"] - df2["benefits"] - df2["change_in_reserve"]
df2["ratio"] = df2["net_income"] / df2["income"]

df2
```

## Closing

This tutorial walked through how a benefit reserve can be calculated using available formulas in the `footings` framework. Do note, that many of the formulas can be combined within a function to show only the final benefit reserve value.
