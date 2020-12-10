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

# Running the Model

```{code-cell} ipython3
import pandas as pd
from footings import (
    model,
    Footing,
    step,
    define_parameter,
    define_sensitivity,
    define_meta,
    define_intermediate,
    define_return
)
from footings.model_tools import create_frame, run_date_time

steps = [
    "_calculate_benefit_end_date",
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
    birth_date = define_parameter()
    benefit_end_age = define_parameter()
    benefit_amount = define_parameter()
    gender = define_parameter()
    interest_rate = define_parameter()
    ctr_sensitivity = define_sensitivity(default=1)
    benefit_end_date = define_intermediate()
    frame = define_return()
    run_date_time = define_meta(meta=run_date_time())

    @step(uses=["birth_date", "benefit_end_age"], impacts=["benefit_end_date"])
    def _calculate_benefit_end_date(self):
        self.benefit_end_date = self.birth_date +\
            pd.DateOffset(years=self.benefit_end_age)

    @step(uses=["valuation_date", "benefit_end_date"], impacts=["frame"])
    def _create_projected_frame(self):
        self.frame = create_frame(
            start_dt=self.valuation_date,
            end_dt=self.benefit_end_date,
            frequency="Y",
            col_date_nm="DATE",
            duration_year="DURATION_YEAR",
        )

    @step(uses=["frame", "gender", "ctr_sensitivity"], impacts=["frame"])
    def _calculate_ctr(self):
        ctrs = {"M": 0.1, "F": 0.1}
        self.frame["CTR"] = ctrs.get(self.gender)
        self.frame["CTR_SENSITIVITY"] = self.ctr_sensitivity
        self.frame["CTR_FINAL"] = self.frame[["CTR", "CTR_SENSITIVITY"]].prod(axis=1)

    @step(uses=["frame"], impacts=["frame"])
    def _calculate_lives_inforce(self):
        self.frame["LIVES_INFORCE"] = (1 - self.frame["CTR_FINAL"]).cumprod()

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
            "RUN_DATE_TIME" : self.run_date_time,
        }
        col_order = [
            "RUN_DATE_TIME",
            "POLICY_ID",
            "CLAIM_ID",
            "CTR_FINAL",
            "LIVES_INFORCE",
            "DISCOUNT_FACTOR",
            "BENEFIT_AMOUNT",
            "DLR",
        ]
        self.frame = self.frame.assign(**cols_added)[col_order]


```
