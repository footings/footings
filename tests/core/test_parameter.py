import pytest

from footings.core.parameter import (
    Parameter,
    ParameterTypeError,
    ParameterAllowedError,
    ParameterMinValueError,
    ParameterMaxValueError,
    ParameterMinLenError,
    ParameterMaxLenError,
    ParameterCustomError,
    define_parameter,
)


def test_argument():
    arg1 = Parameter(
        name="test",
        description="this is a test",
        default=3,
        dtype=int,
        min_val=1,
        max_val=5,
    )
    pytest.raises(ParameterTypeError, arg1.valid, "2")
    pytest.raises(ParameterMinValueError, arg1.valid, 0)
    pytest.raises(ParameterMaxValueError, arg1.valid, 6)
    assert arg1.valid(3) is True

    arg2 = Parameter(
        name="test",
        description="this is a test",
        default=[1, 2, 3],
        dtype=list,
        min_len=1,
        max_len=5,
    )
    pytest.raises(ParameterMinLenError, arg2.valid, [])
    pytest.raises(ParameterMaxLenError, arg2.valid, [1, 2, 3, 4, 5, 6])
    assert arg2.valid([1, 2, 3]) is True

    def is_1(x):
        return x == 1

    arg3 = Parameter(name="test", description="this is a test", custom=is_1)
    pytest.raises(ParameterCustomError, arg3.valid, 2)
    assert arg3.valid(1) is True

    arg4 = Parameter(
        name="test", description="this is a test", dtype=str, allowed=["a", "b", "c"]
    )
    pytest.raises(ParameterAllowedError, arg4.valid, "d")
    assert arg4.valid("a") is True

    # test default
    pytest.raises(
        ParameterTypeError,
        Parameter,
        name="test",
        description="this is a test",
        default="3",
        dtype=int,
    )


def test_define_parameter():
    as_arg = Parameter("a")
    arg = define_parameter("a")
    assert arg == as_arg
