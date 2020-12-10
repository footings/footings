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

# Building a Model

## Introduction

This is the first article in the [Intro Series](index.md) describing how to use the [Footings framework](https://www.github.com/footings/footings) to build models. Note the `Footings framework` was developed with the intention of making it easier to build actuarial models. Thus, the examples provided are going to be focused on actuarial topics, but the `Footings framework` can be used to build any type of model.

To demonstrate the key components to build a model, we are going to build a simple model that calculates the disabled life reserve (DLR) for a given disability insurance policy. A DLR is held by an insurance company insurance once a policy holder meets the disabled criteria and it is equal to the present value of expected future benefits to be paid to the policy holder as a given date (i.e., the valuation date).

Knowing the princples of the `Footings framework` which are liste below will help one understand why models are built the way in which they are.

- Models are a sequence of linked steps
- Models should be easy to understand
- Models need to have validation built in
- Models need to be audited using a second source such as excel
- Models should be self documenting
- Models need to scale when needed
- Models can be combined to form other models

This tutorial won't touch all of the princples, but it will highlight the first two - `models are a sequence of linked steps` and `models should be easy to understand`.

## Model Details

We will assume we have the following information for each policy holder -

- `valuation_date` - The point in time at which the reserve is calculated.
- `policy_id` - The unique id assigned to the disabled policyholder.
- `claim_id` -  The unique id assigned to the disabled claim.
- `benefit_end_date` - The date at which benefits will no longer be paid.
- `benefit_amount` - The monthly benefit amount to be paid.
- `gender` - The gender of the policyholder.
- `interest_rate` - The valuation interest rate to be used in the present value calculation.

A summary of the steps to calculate a DLR for a single policy are below -

1. Create a frame with monthly intervals from the `valuation_date` to `benefit_end_date`.
2. Calculate the claim termination rate (CTR) using the attribute `gender`.
3. Calculate the lives inforce using the CTR.
4. Calculate the discount factor for each interval using the `interest_rate`.
5. Calculate the present value of benefits (i.e., the DLR).
6. Prepare data for output.

## Model Code

Below is the model code to generate the DLR policy model. This code will be used throughout the tutorials in the `Intro Series`. Sometimes the code will be shown as is and other times it will be modified. A description of the code is in the section below.

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

## Explanation

- We start by importing [pandas](https://pandas.pydata.org/) and key functions from the `footings` library.

- Model construction starts on the line `@model`. To build a model with the footings framework, you  need to decorate a standard python class with the `@model` decorator. Our model is called `DLRPolicyModel`. The model decorator takes an argument `steps` which will be explained in bullets below.

- Attributes of the class are defined using the `define_*` functions imported from the `footings` library. These will be covered in further detail in the [attributes and instantiation deep dive tutorial](./2-attributes-and-instantiation.md). But for purposes of this model, know that attributes - defined using `define_parameter` are required parameters to instantiate the model. These attributes are frozen and cannot be modified after the model is instantiated. The frame defined using `define_return` is an empty attribute that is created on instantiation that is unfrozen for modification throughout the steps and returned once the model executes.

- Moving under the attributes, are methods decorated with `@step`. Each step has two arguments - `uses` which is a list of the attributes used for the respective step and `impacts` which is a list of the attributes impacted for the respective step. Viewing some of the code of the methods, you will see we create a pandas DataFrame for the first step (i.e., _create_projected_frame) and steps after modify that frame.

- Returning to the `@model` decorator of the class, the steps argument passed in is a list of the names of the steps defined under the class in the order in which they need to be executed when the mode runs.

## Closing

This tutorial demonstrated the key components used to build a model using the `Footings framework`. One should take away it starts with defining a standard python class, defining attributes, creating methods to represent steps, and finally decorating the class with the `@model` decorator. With the next tutorial, we will do a deeper dive into attributes and instantiation.
