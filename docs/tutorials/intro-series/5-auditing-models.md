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

# Auditing Models
## Introduction

This is the fifth and last tutorial in the [Intro Series](index.md) where we do a deeper dive into auditing models built using the `Footings framework`. Recall, one of the [principles](../../principles.md) of the `Footings framework` is - `models need to be audited using a second source such as excel`. This is important in the actuarial world where models are contrived of many calculations which are used to value and price insurance. Actuaries need to be able to demonstrate how the calculations work to satisfy requirements whether they be internal, external, regulatory, audit, etc.

## Auditing Models

We will continue to use the AddABC model with documentation for the prior tutorial.

We will start with the code from the [prior tutorial](3-documenting-models.md).

```{code-cell} ipython3
from footings import (
    model,
    step,
    def_parameter,
    def_intermediate,
    def_return,
)

@model(steps=["_add_a_b", "_add_ab_c"])
class AddABC:
    """This model takes 3 parameters - a, b, and c and adds them together in two steps."""
    a = def_parameter(dtype=int, description="This is parameter a.")
    b = def_parameter(dtype=int, description="This is parameter b.")
    c = def_parameter(dtype=int, description="This is parameter c.")
    ab = def_intermediate(dtype=int, description="This holds a + b.")
    abc = def_return(dtype=int, description="The sum of ab and c.")

    @step(uses=["a", "b"], impacts=["ab"])
    def _add_a_b(self):
        """Add a and b together and assign to ab."""
        self.ab = self.a + self.b

    @step(uses=["ab", "c"], impacts=["abc"])
    def _add_ab_c(self):
        """Add ab and c together for final return."""
        self.abc = self.ab + self.c
```

To audit a model, one needs to instantiate it and call the `audit` method as shown below. We will first return the audit results as a native python object and in later sections, we will show how it can be exported to different file types.

```{code-cell} ipython3
AddABC(a=1, b=2, c=3).audit()
```

As the output shows, a dictionary is returned with information about the model including arguments passed on initialization, the steps, and the output. Do notice that each step shows output for each attribute that is impacted including the intermediate attributes in the model.

The goal of an audit is to provide the user with sufficient documentation describing what the model does. Using native python code might not be the best delivery format if someone is not a python user. Thus, the `Footings framework` provides the ability to output to a file format. As of right now, two file types are supported - `.xlsx` and `.json`.

The below code shows how to generate an audit file in excel.

```{code-cell} ipython3
AddABC(a=1, b=2, c=3).audit("Audit-AddABC.xlsx")
```

This produces an `.xlsx` file with five tabs. Below are screenshots of the the tabs. The file can also be downloaded {download}`here.<./Audit-AddABC.xlsx>`

```{figure} main.png
Sheet: Main
```

```{figure} instantiation.png
Sheet: Instantiation
```

```{figure} _add_a_b.png
Sheet: _add_a_b
```

```{figure} _add_ab_c.png
Sheet: _add_ab_c
```

```{figure} output.png
Sheet: Output
```

The below code shows how to generate an audit file in excel. The audit file can be downloaded {download}`here.<./Audit-AddABC.json>`

```{code-cell} ipython3
AddABC(a=1, b=2, c=3).audit("Audit-AddABC.json")
```

There are options to configure audit output a certain way. To see this review the audit [api](../../generated/footings.audit.html#module-footings.audit).

## Closing

This tutorial concludes the [Intro Series](index.md) of tutorials. With this tutorial we dug deeper into auditing models which is probably the  most important feature of the `Footings framework`. The long-term goal is to continue to build out this feature so it will be able to generate an audit document sufficient to satisfy the needs of any auditor or regulator who needs an understanding of the model.

Note a more advanced topic of testing with the `Footings framework` builds off of the audit capabilities. At some point there will be a tutorial demonstrating this.
