from attr import attrs, attrib
import pytest

from footings.core.validators import (
    custom_validator,
    min_len_validator,
    max_len_validator,
    min_val_validator,
    max_val_validator,
)


def test_val_validator():
    @attrs
    class TestVal:
        x_min = attrib(validator=min_val_validator(1))
        x_max = attrib(validator=max_val_validator(5))
        x_min_max = attrib(validator=[min_val_validator(1), max_val_validator(5)])

    with pytest.raises(ValueError):
        TestVal(0, 1, 2)
        TestVal(2, 6, 2)
        TestVal(2, 1, 1)

    assert isinstance(TestVal(1, 5, 5), TestVal)


def test_len_validator():
    @attrs
    class TestLen:
        x_min = attrib(validator=min_len_validator(1))
        x_max = attrib(validator=max_len_validator(5))
        x_min_max = attrib(validator=[min_len_validator(1), max_len_validator(5)])

    with pytest.raises(ValueError):
        TestLen("", "abc", "abc")
        TestLen("abc", "abcdef", "abc")
        TestLen("abc", "abc", "abcdef")

    assert isinstance(TestLen("abc", "abc", "abc"), TestLen)


def test_custom_validator():
    def custom_func(value):
        return value < 5

    @attrs
    class TestCustom:
        x = attrib(validator=custom_validator(custom_func))

    pytest.raises(ValueError, TestCustom, 7)
    assert isinstance(TestCustom(1), TestCustom)
