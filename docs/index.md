# Footings

A Model Building Library

---

<br>

## Summary
---
Footings is a model building Python library. No out-of-the box models are provided. Instead it is a framework library that provides key objects and functions to help users  construct custom models.

## Purpose
---
The footings library was developed with the intention of making it easier to develop actuarial models in Python. Actuarial models are a mix of data engineering and math/calculations. In addition, actuarial models are ususally not defined by one calculation, but a series of calculations. So even though the original purpose is actuarial work, if the problem at hand sounds familiar, others might find this library useful.

## Principles
---

The Footings library was designed as framework library using the below princples -

- Models are a sequence of linked steps.
- Models need to have validation built in.
- Models should be easy to understand.
- Models need to be audited using a second source such as excel.
- Models should be self documenting.
- Models need to scale when needed.
- Models can be combined to form other models.


**These all become easier when you can leverage the amazing Python and wider open source ecosystems.**

## Installation
---

This library is not yet on PyPI, but can be installed directly from this repo.

```
pip install git+https://github.com/foootings/footings-core.git
```


## License
---
BSD 3-Clause License

Copyright (c) 2019-2020 Dustin Tindall

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its
   contributors may be used to endorse or promote products derived from
   this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
