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

This is the second tutorial in the [Intro Series](index.md). Here we will do a deeper dive into attributes and instantiation of models.

We will start with the model code from the [prior tutorial](1-building-a-model.md) which is shown below.

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
    a = def_parameter()
    b = def_parameter()
    c = def_parameter()
    ab = def_intermediate()
    abc = def_return()

    @step(uses=["a", "b"], impacts=["ab"])
    def _add_a_b(self):
        self.ab = self.a + self.b

    @step(uses=["ab", "c"], impacts=["abc"])
    def _add_ab_c(self):
        self.abc = self.ab + self.c
```

## Inspecting the Model

To get a better understanding of the model objected, call `getfullargspec` on the `AddABC`.

```{code-cell} ipython3
from inspect import getfullargspec
inspection = getfullargspec(AddABC)
inspection
```

The first thing one should notice is `args` is equal to `[]`. This means there are no arguments that can be passed in without being assigned to a keyword. Looking at the attribute `kwonlyags` we see the 3 parameters - `['a', 'b', 'c']` that were defined using `def_parameter`. Neither `ab` nor `abc` appear in the inspection because they were defined using `def_intermediate` and `def_return` which excludes them from the `__init__` method of the model.

This can be tested by running the following line of code which will return an error.

```{code-cell} ipython3
:tags: [raises-exception]
AddABC(1, 2, 3)
```

The `Footings framework` intentionally make the models key word only objects as many in order to be explicit when models have many parameters.

## An Instantiated Model

To instantiate the model, we will pass arguments using key words as shown below.

```{code-cell} ipython3
model = AddABC(a=1, b=2, c=3)
```

Once a model has been instantiated, the parameters appear as attributes under the model.

```{code-cell} ipython3
print(f"attribute a = {model.a}")
print(f"attribute b = {model.b}")
print(f"attribute c = {model.c}")
```

These attributes are frozen and cannot be modified. The below code will demonstrate this.

```{code-cell} ipython3
:tags: [raises-exception]
model.a = 0
```

In addition, the attributes defined using `def_return` and `def_intermediate` also appear as attributes under the model object. Though, they have a value of `None` as shown below.

```{code-cell} ipython3
print(f"attribute ab  = {model.ab}")
print(f"attribute abc = {model.abc}")
```

These attributes are not frozen so when the model is run the different steps within the model can modify these attributes. This can be tested using the following code.

```{code-cell} ipython3
model.ab = 0
model.ab
```
## Arguments to def_*

Returning to the code of our example model -

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
    a = def_parameter()
    b = def_parameter()
    c = def_parameter()
    ab = def_intermediate()
    abc = def_return()

    @step(uses=["a", "b"], impacts=["ab"])
    def _add_a_b(self):
        self.ab = self.a + self.b

    @step(uses=["ab", "c"], impacts=["abc"])
    def _add_ab_c(self):
        self.abc = self.ab + self.c
```

When calling the `def_*` functions we did not pass any arguments. These functions take a number of optional arguments add the ability to validate data passed to arguments as well as adds to the documentation of the model which will be covered in more detail in the [documentation tutorial](3-documenting-models.md). To see a list of the available arguments you can see the [api section](../../api.rst).

Below is an example of how we can add validation to the model when adding arguments to `def_parameter`.

```{code-cell} ipython3
@model(steps=["_add_a_b", "_add_ab_c"])
class AddABC:
    a = def_parameter(dtype=int, min_val=0)
    b = def_parameter(dtype=int, max_val=0)
    c = def_parameter(dtype=int, allowed=[1, 2])
    ab = def_intermediate()
    abc = def_return()

    @step(uses=["a", "b"], impacts=["ab"])
    def _add_a_b(self):
        self.ab = self.a + self.b

    @step(uses=["ab", "c"], impacts=["abc"])
    def _add_ab_c(self):
        self.abc = self.ab + self.c
```

```{code-cell} ipython3
:tags: [raises-exception]
AddABC(a=-1, b=1, c=3)
```

```{code-cell} ipython3
:tags: [raises-exception]
AddABC(a=1, b=1, c=3)
```

```{code-cell} ipython3
:tags: [raises-exception]
AddABC(a=1, b=0, c=3)
```
## Additional def_* functions

The `footings` library also contains two additional define functions. Both of these will come in handy when building actuarial models.

- `def_meta` is a way to add metadata to a model. As an example, this might be the run date/time a model is ran.

- `def_sensitivity` is a way to add a default parameter. The name sensitivity is often used in actuarial models to test how sensitive an outcome is to a given parameter. As an example, an actuarial model might have an interest rate parameter and an interest rate sensitivity. The default value for the sensitivity would be 1 but could be changed to 1.1 to test the impact of a 10% increase in interest rates.

Both of these are demonstrated in the code below.

```{code-cell} ipython3
from footings import (
    model,
    step,
    def_parameter,
    def_sensitivity,
    def_meta,
    def_intermediate,
    def_return,
)
from footings.model_tools import run_date_time

@model(steps=["_calculate"])
class DiscountFactors:
    interest_rate = def_parameter()
    interest_sensitivity = def_sensitivity(default=1)
    discount_factors = def_return()
    run_date_time = def_meta(meta=run_date_time)

    @step(uses=["interest_rate", "interest_sensitivity"], impacts=["discount_factors"])
    def _calculate(self):
        rate = self.interest_rate * self.interest_sensitivity
        self.discount_factors = [(1-rate)**i for i in range(0, 5)]
```

As the code below shows, `interest_sensitivity` does not need to be set when instantiating the model. It will be assigned a default value of 1.

```{code-cell} ipython3
discount = DiscountFactors(interest_rate=0.05)

print(f"run_date_time = {str(discount.run_date_time)}")
print(f"interest_sensitivity = {str(discount.interest_sensitivity)}")
print(f"output = {str(discount.run())}")
```

It can optionally be changed though as the code below shows.

```{code-cell} ipython3
discount2 = DiscountFactors(interest_rate=0.05, interest_sensitivity=1.1)

print(f"run_date_time = {str(discount2.run_date_time)}")
print(f"interest_sensitivity = {str(discount2.interest_sensitivity)}")
print(f"output = {str(discount2.run())}")
```

## Closing

With this tutorial, we dug deeper into how the `Footings framework` defines attributes and how they are represented in the model. When building models, it is recommended to pass in optional arguments to the `def_*` functions to add validation.

Below is a summary of the functionality of each `def_*` function -

| Define Function     | Init | Default | Frozen | Return |
|:--------------------|:----:|:-------:|:------:|:------:|
| def_parameter       | yes  | no      | yes    | no     |
| def_sensitivity     | yes  | yes     | yes    | no     |
| def_meta            | no   | no      | yes    | no     |
| def_intermediate    | no   | no      | no     | no     |
| def_return          | no   | no      | no     | yes    |
