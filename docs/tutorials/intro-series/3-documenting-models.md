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

# Documenting Models

## Introduction

This is the third tutorial in the [Intro Series](index.md) where we do a deeper dive into the topic of documenting models. One of the underlying [principles](../../principles.md) of the `Footings framework` is *models should be self documenting*.

To start, it is important to note that the `Footings framework` supports numpy style docstrings and internally builds off [numpydoc](https://numpydoc.readthedocs.io/en/latest/) to generate docstrings and the associated objects for generating documentation in sphinx.

## Documented Model

Below is the code for `AddABC` from the prior two tutorials with documentations.

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

As the above code shows -

- Documentation about the model is added as a standard python docstring directly underneath the class.
- Documentation for attributes, including the data types, should be set within the `def_*` function.
- Information on each step can be added as a docstring underneath the method defining that step.

Do note the attributes could be documented in the associated docstring. However, it is recommended to put the attribute documentation within the `def_*` function. Lastly, being able to pass documentation (i.e., dtype and description) into functions as code allows us to move the source of the documentation to be elsewhere.

The below code demonstrates this. Assume the model developer wants to have a data dictionary file that is the true source of information underlying the parameters.

```{code-cell} ipython3
import yaml

data_dictionary_file = """
a:
  dtype: int
  description: This is parameter a.
b:
  dtype: int
  description: This is parameter b.
c:
  dtype: int
  description: This is parameter c.
"""

data_dictionary = yaml.safe_load(data_dictionary_file)

data_dictionary
```

We can then pass the information from the data dictionary file into the associated parameters.

```{code-cell} ipython3
@model(steps=["_add_a_b", "_add_ab_c"])
class AddABC:
    """This model takes 3 parameters - a, b, and c and adds them together in two steps."""
    a = def_parameter(**data_dictionary["a"])
    b = def_parameter(**data_dictionary["b"])
    c = def_parameter(**data_dictionary["c"])
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

## Viewing the Documentation

To see what the documentation looks like to a user call `help` on the model.

```{code-cell} ipython3
help(AddABC)
```

When viewing the output, you will see the attributes are split into sections based on the `def_*` function called. In addition, directly below the return detail is a section titled `Steps` with the method name of the step and the description of the step.

This layout of the  documentation will flow through if using [sphinx](https://www.sphinx-doc.org/en/master/). Be sure to include `footings.doc_tools` as an extension in your `conf.py` file. For an example of this see the `footings-idi-model` [repository](https://github.com/footings/footings-idi-model) and [documentation](https://footings-idi-model.readthedocs.io/en/master/).

## Closing

When building a model using the `Footings framework`, the developer needs to document the different components of the model and the `footings` library will organize those components into consumable documentation. Model documentation is paired best with sphinx and a CI/CD pipeline producing versioned documentation along side your model code.
