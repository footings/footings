from typing import Any, List, Union, Optional, Callable
from datetime import date, datetime

from attr import attrs, attrib


class ParameterTypeError(Exception):
    """Wrong type passed to parameter"""


def _check_type(expected, value):
    if isinstance(value, expected) is False:
        raise ParameterTypeError(f"Expected type {value} but received {type(value)}")


class ParameterAllowedError(Exception):
    """Value not allowed passed to parameter"""


def _check_allowed(allowed, value):
    if value not in allowed:
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

TYPE_MAP = {
    "int": int,
    "float": float,
    "date": date,
    "datetime": datetime,
    "str": str,
    "bool": bool,
}


def _convert_type(x):
    if isinstance(x, str):
        val = TYPE_MAP.get(x, None)
        if val is None:
            msg = (
                f"{x} was passed as a str with no associated mapping. See documentation."
            )
            raise ValueError(msg)
        return val
    return x


@attrs(frozen=True, slots=True, repr=False)
class Parameter:
    """A parameter to be passed to a model.

    A parameter has built in validation that is called on instantiation of a model. The attributes dtype through
    custom are validators that can be used to specify characteristics of the parameter.

    Attributes
    ----------
    name : str
        Name to assign the parameter. Will show in docstring of created models.
    description : str
        A description of the parameter. Will show in docstring of created models.
    default : Any, optional
        The default value when a value is not passed to parameter in a model.
    dtype : str, type, optional
        A validator for the expected type of the passed parameter value.
    allowed : List[Any], optional
        A validator setting the allowed values to be passed.
    min_val : int, float, optional
        A validator for the minimum value allowed.
    max_val : int, float, optional
        A validator for the maximum value allowed.
    min_len : int, optional
        A value for the minimum length allowed.
    max_len : int, optional
        A value for the maximum length allowed.
    custom : callable
        A custom function to use as a validator.
    """

    name: str = attrib()
    description: Optional[str] = attrib(default=None, repr=False)
    default: Optional[Any] = attrib(default=None)
    dtype: Optional[Union[str, type]] = attrib(default=None, converter=_convert_type)
    allowed: Optional[List[Any]] = attrib(default=None)
    min_val: Optional[Union[int, float]] = attrib(default=None)
    max_val: Optional[Union[int, float]] = attrib(default=None)
    min_len: Optional[int] = attrib(default=None)
    max_len: Optional[int] = attrib(default=None)
    custom: Optional[Callable] = attrib(default=None)

    def __attrs_post_init__(self):
        if self.default is not None:
            self.valid(self.default)

    def valid(self, value):
        """Test to see if value is valid as a parameter"""
        for k, v in _PARAMS_CHECKS.items():
            if getattr(self, k) is not None:
                v(getattr(self, k), value)
        return True

    def _create_validator(self):
        def validator(inst, attribute, value):
            return self.valid(value)

        return validator


def define_parameter(name: str, **kwargs) -> Parameter:
    """A factory function to define a Parameter.

    A parameter is used to construct models.

    Parameters
    ----------
    name : str
        Name to assign the parameter (appears in docstring of created model).
    **kwargs
        The keywords passed to a Parameter. See Parameter attributes for details.

    Returns
    -------
    Parameter
        An instance of parameter.

    See Also
    --------
    footings.parameter.Parameter

    Examples
    --------
    >>> from footings import create_parameter
    >>> arg = define_parameter(name="arg", dtype=str)
    >>> arg.valid("test") # returns True
    """

    return Parameter(name, **kwargs)
