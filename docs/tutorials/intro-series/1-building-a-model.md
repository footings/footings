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

This is the first tutorial in the [Intro Series](index.md) describing how to use the [Footings framework](https://www.github.com/footings/footings) to build models. Note the `Footings framework` was developed with the intention of making it easier to build actuarial models, but it can also be used to build any type of model.

Knowing the [principles](../../principles.md) of the `Footings framework` will help one understand why models are built the way in which they are. This tutorial won't touch all of the principles, but it will highlight the first two - `models are a sequence of linked steps` and `models should be easy to understand`.

## Example Model Code

We will start by building a simple model that takes 3 parameters - `a`, `b`, and `c` and adds them together. The addition will be broken up into two steps where we first add `a` and `b` and then add the results of the first step to `c` to get the final result. This is purely to show how some of the functions work in the `Footings framework`.

The code for the  model is below.

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


## Example Model Explanation

- A model build starts with importing key functions from the `footings` library.

- Model construction starts on the line `@model`. To build a model with the footings framework, you  need to decorate a standard python class with the `@model` decorator. Our model is called `AddABC`. The model decorator takes an argument `steps` which is explained in the last bullet.

- Attributes of the class are defined using the `def_*` functions imported from the `footings` library. These will be covered in further detail in the [attributes and instantiation tutorial](./2-attributes-and-instantiation.md). But for purposes of this model, know that attributes - defined using `def_parameter` are required parameters to instantiate the model. These attributes are frozen and cannot be modified after the model is instantiated. Attributes defined using `def_intermediate` and `def_return` are empty attributes that are created on instantiation that are unfrozen for modification throughout the steps. The only difference between the two is `def_return` attributes are returned after the model runs.

- Moving under the attributes, are methods decorated with `@step`. Each step has two arguments - `uses` which is a list of the attributes used for the respective step and `impacts` which is a list of the attributes impacted for the respective step.

- Returning to the `@model` decorator of the class, the steps argument passed in is a list of the names of the steps defined under the class in the order in which they need to be executed when the mode runs.

## Running the Model

To run the model, we first need to instantiate it and call the `run` method as shown below.

```{code-cell} ipython3
AddABC(a=1, b=2, c=3).run()
```

The model returns the single digit 6 which was the value assigned to the attribute `abc` in the second step as it was defined as the `return` attribute. In the [Running Models](4-running-models.md) tutorial, we will dive further into the topic of running models.

## Closing

This tutorial demonstrated the key components used to build a model using the `Footings framework`. One should take away it starts with defining a standard python class, defining attributes, creating methods to represent steps, and finally decorating the class with the `@model` decorator. With the next tutorial, we will do a deeper dive into attributes and instantiation.
