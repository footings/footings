from attr import attrs, attrib
import pytest

from footings.validators import (
    value_min,
    value_max,
    value_in_range,
    len_min,
    len_max,
    len_in_range,
)


def test_val_validator():
    @attrs
    class TestVal:
        x_min = attrib(validator=value_min(1))
        x_max = attrib(validator=value_max(5))
        x_in_range = attrib(validator=value_in_range(1, 5))

    with pytest.raises(ValueError):
        TestVal(0, 1, 2)
        TestVal(2, 6, 2)
        TestVal(2, 1, 1)

    assert isinstance(TestVal(1, 5, 5), TestVal)


def test_len_validator():
    @attrs
    class TestLen:
        x_min = attrib(validator=len_min(1))
        x_max = attrib(validator=len_max(5))
        x_min_max = attrib(validator=len_in_range(1, 5))

    with pytest.raises(ValueError):
        TestLen("", "abc", "abc")
        TestLen("abc", "abcdef", "abc")
        TestLen("abc", "abc", "abcdef")

    assert isinstance(TestLen("abc", "abc", "abc"), TestLen)
