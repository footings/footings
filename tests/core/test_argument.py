import pytest

from footings.core.argument import (
    Argument,
    ArgumentTypeError,
    ArgumentAllowedError,
    ArgumentMinValueError,
    ArgumentMaxValueError,
    ArgumentMinLenError,
    ArgumentMaxLenError,
    ArgumentCustomError,
    create_argument,
)


def test_argument():
    arg1 = Argument(
        name="test",
        description="this is a test",
        default=3,
        dtype=int,
        min_val=1,
        max_val=5,
    )
    pytest.raises(ArgumentTypeError, arg1.valid, "2")
    pytest.raises(ArgumentMinValueError, arg1.valid, 0)
    pytest.raises(ArgumentMaxValueError, arg1.valid, 6)
    assert arg1.valid(3) is True

    arg2 = Argument(
        name="test",
        description="this is a test",
        default=[1, 2, 3],
        dtype=list,
        min_len=1,
        max_len=5,
    )
    pytest.raises(ArgumentMinLenError, arg2.valid, [])
    pytest.raises(ArgumentMaxLenError, arg2.valid, [1, 2, 3, 4, 5, 6])
    assert arg2.valid([1, 2, 3]) is True

    def is_1(x):
        return x == 1

    arg3 = Argument(name="test", description="this is a test", custom=is_1)
    pytest.raises(ArgumentCustomError, arg3.valid, 2)
    assert arg3.valid(1) is True

    arg4 = Argument(
        name="test", description="this is a test", dtype=str, allowed=["a", "b", "c"]
    )
    pytest.raises(ArgumentAllowedError, arg4.valid, "d")
    assert arg4.valid("a") is True

    # test default
    pytest.raises(
        ArgumentTypeError,
        Argument,
        name="test",
        description="this is a test",
        default="3",
        dtype=int,
    )


def test_create_argument():
    as_arg = Argument("a")
    arg = create_argument("a")
    assert arg == as_arg
