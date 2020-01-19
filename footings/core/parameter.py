"""parameter.py"""

from typing import Any, List, Dict, Union, Optional, Callable
from dataclasses import dataclass, field


class ParameterTypeError(Exception):
    """Wrong type passed to parameter"""


def _check_type(expected, value):
    if isinstance(value, expected) is False:
        raise ParameterTypeError(f"Expected type {value} but received {type(value)}")


class ParameterAllowedError(Exception):
    """Value not allowed passed to parameter"""


def _check_allowed(allowed, value):
    if value in allowed is False:
        raise ParameterAllowedError(f"{value} is not in [{allowed}]")


class ParameterMinValueError(Exception):
    """Value below allowed minimum passed to parameter"""


def _check_min_val(min_val, value):
    if value < min_val:
        raise ParameterMinValueError(f"{value} is less than {min_val}")


class ParameterMaxValueError(Exception):
    """Value above allowed maximum passed to parameter"""


def _check_max_val(max_val, value):
    if value > max_val:
        raise ParameterMaxValueError(f"{value} is greater than {max_val}")


class ParameterMinLenError(Exception):
    """Value with length below allowed minimum passed to parameter"""


def _check_min_len(min_len, value):
    if len(value) < min_len:
        raise ParameterMinLenError(f"len {len(value)} is less than {min_len}")


class ParameterMaxLenError(Exception):
    """Value with length above allowed maximum passed to parameter"""


def _check_max_len(max_len, value):
    if len(value) > max_len:
        raise ParameterMaxLenError(f"len {len(value)} is greater than {max_len}")


class ParameterCustomError(Exception):
    """Value fails custom test"""


def _check_custom(func, value):
    if callable(func) is False:
        raise TypeError("the object passed to custom must be callable")

    if func(value) is False:
        raise ParameterCustomError(f"The custom test failed with {value}")


@dataclass
class Parameter:
    """ """

    name: str
    description: Optional[str] = field(default=None, repr=False)
    default: Optional[Any] = field(default=None)
    dtype: Optional[type] = field(default=None)
    allowed: Optional[List[Any]] = field(default=None)
    min_val: Optional[Union[int, float]] = field(default=None)
    max_val: Optional[Union[int, float]] = field(default=None)
    min_len: Optional[int] = field(default=None)
    max_len: Optional[int] = field(default=None)
    custom: Optional[Callable] = field(default=None)
    other_meta: Dict = field(default_factory=lambda: {}, repr=False)

    def __post_init__(self):
        if self.default is not None:
            self.valid(self.default)

    def valid(self, value):
        if self.dtype is not None:
            _check_type(self.dtype, value)

        if self.allowed is not None:
            _check_allowed(self.allowed, value)

        if self.min_val is not None:
            _check_min_val(self.min_val, value)

        if self.max_val is not None:
            _check_max_val(self.max_val, value)

        if self.min_len is not None:
            _check_min_len(self.min_len, value)

        if self.max_len is not None:
            _check_max_len(self.max_len, value)

        if self.custom is not None:
            _check_custom(self.custom, value)

        return True

    def generate_meta(self):
        return {
            "description": self.description,
            "default": self.default,
            "dtype": self.dtype,
            "allowed": self.allowed,
            "min_val": self.min_val,
            "max_val": self.max_val,
            "min_len": self.min_len,
            "max_len": self.max_len,
            "custom": self.custom,  # probably want to get docstring or turn this into a str
            **self.other_meta,
        }
