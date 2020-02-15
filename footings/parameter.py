"""Objects tied to creating Parameters"""

from typing import Any, List, Dict, Union, Optional, Callable
from attr import attrs, attrib


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


_PARAMS_CHECKS = {
    "dtype": _check_type,
    "allowed": _check_allowed,
    "min_val": _check_min_val,
    "max_val": _check_max_val,
    "min_len": _check_min_len,
    "max_len": _check_max_len,
    "custom": _check_custom,
}


@attrs(frozen=True, slots=True)
class Parameter:
    """Parameter"""

    # pylint: disable=too-many-instance-attributes
    name: str = attrib()
    description: Optional[str] = attrib(default=None, repr=False)
    default: Optional[Any] = attrib(default=None)
    dtype: Optional[type] = attrib(default=None)
    allowed: Optional[List[Any]] = attrib(default=None)
    min_val: Optional[Union[int, float]] = attrib(default=None)
    max_val: Optional[Union[int, float]] = attrib(default=None)
    min_len: Optional[int] = attrib(default=None)
    max_len: Optional[int] = attrib(default=None)
    custom: Optional[Callable] = attrib(default=None)
    other_meta: Dict = attrib(factory=dict, repr=False)

    def __attrs_post_init__(self):
        if self.default is not None:
            self.valid(self.default)

    def valid(self, value):
        """Test to see if value is valid as a parameter"""
        for k, v in _PARAMS_CHECKS.items():
            if getattr(self, k) is not None:
                v(getattr(self, k), value)
        return True

    def create_validator(self):
        """Create validator for table"""

        def validator(inst, attribute, value):
            return self.valid(value)

        return validator

    def generate_meta(self):
        """Generate meta information for parameter"""
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
