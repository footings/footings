"""Test for parameter.py"""

# pylint: disable=function-redefined, missing-function-docstring

import pytest

from footings.argument import (
    Argument,
    ArgumentTypeError,
    ArgumentAllowedError,
    ArgumentMinValueError,
    ArgumentMaxValueError,
    ArgumentMinLenError,
    ArgumentMaxLenError,
    ArgumentCustomError,
)


def test_parameter():
    param1 = Argument(
        name="test",
        description="this is a test",
        default=3,
        dtype=int,
        min_val=1,
        max_val=5,
    )
    pytest.raises(ArgumentTypeError, param1.valid, "2")
    pytest.raises(ArgumentMinValueError, param1.valid, 0)
    pytest.raises(ArgumentMaxValueError, param1.valid, 6)
    assert param1.valid(3) is True

    param2 = Argument(
        name="test",
        description="this is a test",
        default=[1, 2, 3],
        dtype=list,
        min_len=1,
        max_len=5,
    )
    pytest.raises(ArgumentMinLenError, param2.valid, [])
    pytest.raises(ArgumentMaxLenError, param2.valid, [1, 2, 3, 4, 5, 6])
    assert param2.valid([1, 2, 3]) is True

    def is_1(x):
        return x == 1

    param3 = Argument(name="test", description="this is a test", custom=is_1)
    pytest.raises(ArgumentCustomError, param3.valid, 2)
    assert param3.valid(1) is True

    # test default
    pytest.raises(
        ArgumentTypeError,
        Argument,
        name="test",
        description="this is a test",
        default="3",
        dtype=int,
    )
