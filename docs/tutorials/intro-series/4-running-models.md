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

# Running Models

## Introduction

This is the fourth tutorial in the [Intro Series](index.md) where we do a deeper dive into running models built using the `Footings framework`.

## Running Models

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

As shown in the [first tutorial](1-building-a-model.md), we can call the `run` method to run the model.

```{code-cell} ipython3
AddABC(a=1, b=2, c=3).run()
```

The `run` method does take one optional argument `to_step` which you can pass the step to which you want to run the model too. This can be useful for debugging purposes.

```{code-cell} ipython3
to_ab = AddABC(a=1, b=2, c=3).run(to_step="_add_a_b")
print(f"AddABC.ab  = {str(to_ab.ab)}")
print(f"AddABC.abc = {str(to_ab.abc)}")
```

As the code above shows, running the model to step `_add_a_b` assigns the ab the value 3 but leaves abc `None` because it is not touched until the next step.

## Models with Multiple Return Attributes

If a model has multiple return attributes, the model will return a tuple of the attributes. This is demonstrated below. Note we changed the attribute `ab` from using `def_intermediate` to `def_return`.

```{code-cell} ipython3
@model(steps=["_add_a_b", "_add_ab_c"])
class AddABC:
    """This model takes 3 parameters - a, b, and c and adds them together in two steps."""
    a = def_parameter(dtype=int, description="This is parameter a.")
    b = def_parameter(dtype=int, description="This is parameter b.")
    c = def_parameter(dtype=int, description="This is parameter c.")
    ab = def_return(dtype=int, description="This holds a + b.")
    abc = def_return(dtype=int, description="The sum of ab and c.")

    @step(uses=["a", "b"], impacts=["ab"])
    def _add_a_b(self):
        """Add a and b together and assign to ab."""
        self.ab = self.a + self.b

    @step(uses=["ab", "c"], impacts=["abc"])
    def _add_ab_c(self):
        """Add ab and c together for final return."""
        self.abc = self.ab + self.c

AddABC(a=1, b=2, c=3).run()
```

Do note that the return attributes are returned in order of declaration within the model.

## Closing

With this tutorial, we dug a little deeper into running models and the possibility of returning multiple attributes upon execution of a model. In the next tutorial, we will look at how we can `audit` models and see what each step does as the model runs.
