import pytest

from footings.parameter import (
    Parameter,
    ParameterTypeError,
    ParameterAllowedError,
    ParameterMinValueError,
    ParameterMaxValueError,
    ParameterMinLenError,
    ParameterMaxLenError,
    ParameterCustomError,
)


def test_parameter():
    param1 = Parameter(
        name="test",
        description="this is a test",
        default=3,
        dtype=int,
        min_val=1,
        max_val=5,
    )
    pytest.raises(ParameterTypeError, param1.valid, "2")
    pytest.raises(ParameterMinValueError, param1.valid, 0)
    pytest.raises(ParameterMaxValueError, param1.valid, 6)
    assert param1.valid(3) == True

    param2 = Parameter(
        name="test",
        description="this is a test",
        default=[1, 2, 3],
        dtype=list,
        min_len=1,
        max_len=5,
    )
    pytest.raises(ParameterMinLenError, param2.valid, [])
    pytest.raises(ParameterMaxLenError, param2.valid, [1, 2, 3, 4, 5, 6])
    assert param2.valid([1, 2, 3]) == True

    def is_1(x):
        return x == 1

    param3 = Parameter(name="test", description="this is a test", custom=is_1)
    pytest.raises(ParameterCustomError, param3.valid, 2)
    assert param3.valid(1) == True

    # test default
    pytest.raises(
        ParameterTypeError,
        Parameter,
        name="test",
        description="this is a test",
        default="3",
        dtype=int,
    )
