# Footings - A Model Building Library

![tests](https://github.com/footings/footings/workflows/tests/badge.svg)
[![gh-pages](https://github.com/footings/footings/workflows/gh-pages/badge.svg)](https://footings.github.io/footings/master/)
[![license](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)
[![codecov](https://codecov.io/gh/dustindall/footings/branch/master/graph/badge.svg?token=SC5BHMYBSN)](https://codecov.io/gh/dustindall/footings)
![version](https://img.shields.io/pypi/pyversions/footings.svg)
[![PyPI version](https://badge.fury.io/py/footings.svg)](https://badge.fury.io/py/footings)

## What is it?

Footings is a model building Python library. No out-of-the box models are provided. Instead it is a framework library that provides key objects and functions to help users  construct custom models.

## Purpose

The footings library was developed with the intention of making it easier to develop actuarial models in Python. Actuarial models are a mix of data engineering and math/calculations. In addition, actuarial models are usually not defined by one calculation, but a series of calculations. So even though the original purpose is actuarial work, if the problem at hand sounds familiar, others might find this library useful.

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

**These all become easier when you can leverage the amazing Python and wider open source ecosystems.**

## Installation

To install from PyPI run -

```
pip install footings
```

To install the latest version on github run -

```
pip install git+https://github.com/foootings/footings-core.git
```

## License
[BSD 3](LICENSE)


## Changelog

[File](./docs/changelog.md)
