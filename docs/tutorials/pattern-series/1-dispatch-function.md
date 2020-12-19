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

# Function Dispatch

## Introduction

Often times when building actuarial models, we want arguments to trigger different functions. A good example of this is when we have multiple assumption sets (e.g., stat, gaap, best-estimate) supporting a model and when we run the model we need to trigger functions that read data from the respective assumption set. This pattern can be coded using an ifelse block

```{code-cell} ipython3
# if assumption_set == "stat":
#     assumptions = load_assumptions("./stat/")
# elif assumption_set == "gaap":
#     assumptions = load_assumptions("./gaap/")
# elif assumption_se == "best-estimate":
#     assumptions = load_assumptions("./best-estimate/")
```

However, each time an assumption set is added, one needs to go into the code block and add the respective logic. Another pattern exists within python using decorators. An example of this is how the [singledispatch](https://docs.python.org/3/glossary.html#term-single-dispatch) function works. This is preferred because it does not require entering the body of a function to change an ifelse block.

## Dispatch Function

The `footings` library has a function called [dispatch_function](../../generated/footings.dispatch_function.html#footings.dispatch_function) that can be used to solve the problem of creating a single function that calls different functions based on passed parameters. An example of using this is shown in the code below.

```{code-cell} ipython3
import pandas as pd
from footings import dispatch_function

@dispatch_function(key_parameters=("reporting_type",))
def add_ctr(reporting_type, frame):
    raise NotImplementedError("No default function is implemented")

@add_ctr.register(reporting_type="STAT")
def _(frame):
    frame["CTR"] = 0.02
    return frame

@add_ctr.register(reporting_type="GAAP")
def _(frame):
    frame["CTR"] = 0.015
    return frame

@add_ctr.register(reporting_type="BEST-ESTIMATE")
def _(frame):
    frame["CTR"] = 0.01
    return frame


frame = pd.DataFrame({"DURATION": range(1, 5)})

frame
```

```{code-cell} ipython3
add_ctr(reporting_type="STAT", frame=frame)
```

```{code-cell} ipython3
add_ctr(reporting_type="GAAP", frame=frame)
```

```{code-cell} ipython3
add_ctr(reporting_type="BEST-ESTIMATE", frame=frame)
```

## Closing

Using `dispatch_function` as a pattern is useful when one is trying to create a single function that has the capability to call different functions based on the passed parameters.
