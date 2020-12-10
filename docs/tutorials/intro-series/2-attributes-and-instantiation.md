---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
kernelspec:
  display_name: Python 3
  language: python
  name: python3

execution:
  timeout: -1
---

# Attributes and Instantiation

## Introduction

This is the second tutorial in the [Intro Series](index.md). Here we will do a deep dive on attributes and instantiation.

We will start with the model code from the [building a model tutorial](1-build-a-model.md) which is shown below.

```{code-cell} ipython3
import pandas as pd
from footings import (
    model,
    Footing,
    step,
    define_parameter,
    define_return
)
from footings.model_tools import create_frame

steps = [
    "_create_projected_frame",
    "_calculate_ctr",
    "_calculate_lives_inforce",
    "_calculate_discount_factor",
    "_calculate_dlr",
    "_prepare_final_output",
]

@model(steps=steps)
class DLRPolicyModel(Footing):
    valuation_date = define_parameter()
    policy_id = define_parameter()
    claim_id = define_parameter()
    benefit_end_date = define_parameter()
    benefit_amount = define_parameter()
    gender = define_parameter()
    interest_rate = define_parameter()
    frame = define_return()

    @step(uses=["valuation_date", "benefit_end_date"], impacts=["frame"])
    def _create_projected_frame(self):
        self.frame = create_frame(
            start_dt=self.valuation_date,
            end_dt=self.benefit_end_date,
            frequency="Y",
            col_date_nm="DATE",
            duration_year="DURATION_YEAR",
        )

    @step(uses=["frame", "gender"], impacts=["frame"])
    def _calculate_ctr(self):
        ctrs = {"M": 0.1, "F": 0.1}
        self.frame["CTR"] = ctrs.get(self.gender)

    @step(uses=["frame"], impacts=["frame"])
    def _calculate_lives_inforce(self):
        self.frame["LIVES_INFORCE"] = (1 - self.frame["CTR"]).cumprod()

    @step(uses=["frame", "interest_rate"], impacts=["frame"])
    def _calculate_discount_factor(self):
        self.frame["DISCOUNT_FACTOR"] = (
            1 / (1 + self.interest_rate) ** self.frame["DURATION_YEAR"]
        )

    @step(uses=["frame", "benefit_amount"], impacts=["frame"])
    def _calculate_dlr(self):
        prod_cols = ["LIVES_INFORCE", "DISCOUNT_FACTOR", "BENEFIT_AMOUNT"]
        self.frame["BENEFIT_AMOUNT"] = self.benefit_amount
        self.frame["DLR"] = self.frame[prod_cols].prod().iloc[::-1].cumsum()

    @step(uses=["frame", "policy_id", "claim_id"], impacts=["frame"])
    def _prepare_final_output(self):
        cols_added = {
            "POLICY_ID": self.policy_id,
            "CLAIM_ID": self.claim_id,
        }
        col_order = [
            "POLICY_ID",
            "CLAIM_ID",
            "CTR",
            "LIVES_INFORCE",
            "DISCOUNT_FACTOR",
            "BENEFIT_AMOUNT",
            "DLR",
        ]
        self.frame = self.frame.assign(**cols_added)[col_order]

```

## Instantiation of the Model

Before we instantiate the model, call `getfullargspec` on the model.

```{code-cell} ipython3
from inspect import getfullargspec
inspection = getfullargspec(DLRPolicyModel)
inspection
```

The first thing to notice is that models created with `Footings framework` are to be instantiated with key word arguments only. This is observable as `args` is equal to `[]` and `kwonlyargs` is equal to the attributes we defined using `define_parameter`. This is demonstrated when trying to run the code below.

```{code-cell} ipython3
DLRPolicyModel(pd.Timestamp())
```

## Instantiation of the Model

## Adding Arguments to define_*

## Closing
