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

# Documentation


```{code-cell} ipython3
import pandas as pd
from footings import model, Footing, step, define_parameter, define_return

steps = [
    "_create_projected_frame",
    "_lookup_ctr",
    "_calculate_lives_inforce",
    "_calculcate_discount",
    "_calculate_pv_benefits",
    "_prepare_final_output",
]
@model(steps=steps)
class DLRPolicyModel(Footing):
    policy_id = define_parameter(
        dtype=str,
        description="The unique id assigned to the policyholder."
    )
    claim_id = define_parameter(dtype=str, description="The unique id assigned tot he claim.")
    valuation_date = define_parameter(dtype=pd.Timestamp, description="The point in time at which the reserve is calculated.")
    benefit_end_date = define_parameter(dtype=pd.Timestamp, description="The date benefits will no longer be paid (assume 65th birthday).")
    benefit_amount = define_parameter(dtype=float, description="The monthly benefit amount to be paid.")
    gender = define_parameter(dtype=str, description="The gender of the policyholder.", allowed=["M", "F"])
    occupation_class = define_parameter(dtype=str, description="The occupation class of the policyholder., allowed=["1", "2"])
    interest_rate = define_parameter(dtype=float, description="The valuation interest rate to be used in the present value calculation.)
    frame = define_return(dtype=pd.DataFrame, description="The frame to be returned by the model")

    @step(uses=["valuation_date", "benefit_end_date"], impacts=["frame"])
    def _create_projected_frame(self):
        pass

    @step(uses=["frame", "gender", "occupation_class"], imapacts=["frame"])
    def _lookup_ctr(self):
        pass

    @step(uses=["frame"], imapacts=["frame"])
    def _calculate_lives_inforce(self):
        pass

    @step(uses=["frame", "interest_rate"], imapacts=["frame"])
    def _calcualte_discount(self):
        pass

    @step(uses=["frame"], imapacts=["frame"])
    def _calculate_pv_benefits(self):
        pass

    @step(uses=["frame", "policy_id", "claim_id"], imapacts=["frame"])
    def _prepare_final_output(self):
        pass


```
