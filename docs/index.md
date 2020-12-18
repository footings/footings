
# Footings

*A Model Building Library*

## Summary

Footings is a model building Python library. No out-of-the box models are provided. Instead it is a framework library that provides key objects and functions to help users  construct custom models.

## Purpose

The footings library was developed with the intention of making it easier to develop actuarial models in Python. Actuarial models are a mix of data engineering and math/calculations. In addition, actuarial models are ususally not defined by one calculation, but a series of calculations. So even though the original purpose is actuarial work, if the problem at hand sounds familiar, others might find this library useful.

## Principles

The Footings library was designed as framework library using the below principles -

- Models are a sequence of linked steps
- Models need to be easy to understand
- Models need to have validation built in
- Models need to be easy to audit
- Models need to be self documenting
- Models need to be able to scale up
- Models need to be able to build off other models
- Model environments should not be monolithic

**These all become easier when you can leverage the amazing Python and wider open source ecosystems as well.**

For a more insights into principles see [principles](principles.md)

## Learning

The best way to learn how to use the footings library to build models is to read through the [tutorials](tutorials/index.md).


```{toctree}
:maxdepth: 2
:hidden:

installation.md
principles.md
tutorials/index.md
api.rst
license.md
changelog.md
```
