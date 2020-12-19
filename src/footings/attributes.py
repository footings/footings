from typing import Any

from attr import attrib, Factory
from attr._make import _CountingAttr
from attr.validators import instance_of, in_
from attr.setters import NO_OP

from footings.validators import (
    custom_validator,
    min_len_validator,
    max_len_validator,
    min_val_validator,
    max_val_validator,
)


class Parameter:
    pass


class Sensitivity:
    pass


class Meta:
    pass


class Intermediate:
    pass


class Return:
    pass


VALIDATOR_MAPPING = {
    "dtype": instance_of,
    "allowed": in_,
    "custom": custom_validator,
    "min_len": min_len_validator,
    "max_len": max_len_validator,
    "min_val": min_val_validator,
    "max_val": max_val_validator,
}


def _get_validators(**kwargs):
    return [VALIDATOR_MAPPING[k](v) for k, v in kwargs.items() if k in VALIDATOR_MAPPING]


def _define(
    footing_group: str, init: bool, dtype: type, description: str, frozen: bool, **kwargs,
):
    validators = _get_validators(**kwargs)
    kwargs = {k: v for k, v in kwargs.items() if k not in VALIDATOR_MAPPING}
    metadata = {
        "description": description if description is not None else "",
        "footing_group": footing_group,
    }
    on_setattr = NO_OP if frozen is False else None

    return attrib(
        init=init,
        type=dtype,
        repr=False,
        validator=validators,
        metadata=metadata,
        on_setattr=on_setattr,
        **kwargs,
    )


def def_return(*, dtype=None, description=None, default=None, **kwargs) -> _CountingAttr:
    """Define an return to the model where an return is a non-frozen attribute that is
    created by the model and when the model runs.

    Parameters
    ----------
    dtype : type
        The expected type of the attribute. If not None, value will be validated on instantiation.
    description : str
        The description of the attribute.
    default : Any
        The default value of the attribute.
    kwargs : dict
        Any one of the following validators - allowed, custom, min_val, max_val, min_len, and max_len.

    Returns
    -------
    _CoutningAttr
        An attribute that is recognized by the model.
    """
    return _define(
        footing_group=Return(),
        init=False,
        dtype=dtype,
        description=description,
        default=default,
        frozen=False,
        **kwargs,
    )


def def_intermediate(
    *, dtype=None, description=None, default=None, **kwargs
) -> _CountingAttr:
    """Define a placeholder to the model where a placeholder is a non-frozen attribute that is
    created by the model and not returned when the model runs.

    Parameters
    ----------
    dtype : type
        The expected type of the attribute. If not None, value will be validated on instantiation.
    description : str
        The description of the attribute.
    default : Any
        The default value of the attribute.
    kwargs : dict
        Any one of the following validators - allowed, custom, min_val, max_val, min_len, and max_len.

    Returns
    -------
    _CoutningAttr
        An attribute that is recognized by the model.
    """
    return _define(
        footing_group=Intermediate(),
        init=False,
        dtype=dtype,
        description=description,
        default=default,
        frozen=False,
        **kwargs,
    )


def def_meta(
    *, meta: Any, dtype=None, description=None, default=None, **kwargs
) -> _CountingAttr:
    """Define meta data for the model which is a frozen attribute that is passed on instantiation of the model.

    Parameters
    ----------
    meta : Any
        Any value to be defined as meta data for the model.
    dtype : type
        The expected type of the attribute. If not None, value will be validated on instantiation.
    description : str
        The description of the attribute.
    default : Any
        The default value of the attribute.
    kwargs : dict
        Any one of the following validators - allowed, custom, min_val, max_val, min_len, and max_len.

    Returns
    -------
    _CoutningAttr
        An attribute that is recognized by the model.
    """
    return _define(
        footing_group=Meta(),
        init=False,
        dtype=dtype,
        description=description,
        default=Factory(meta) if callable(meta) else meta,
        frozen=True,
        **kwargs,
    )


def def_sensitivity(
    *, default: Any, dtype=None, description=None, **kwargs
) -> _CountingAttr:
    """Define a modifer to the model where a modifier is a frozen attribute with a required default value.

    A modifier is intended to be used to modify or test sensitvities of parameters within the model.

    Parameters
    ----------
    default : Any
        The default value of the attribute.
    dtype : type
        The expected type of the attribute. If not None, value will be validated on instantiation.
    description : str
        The description of the attribute.
    kwargs : dict
        Any one of the following validators - allowed, custom, min_val, max_val, min_len, and max_len.

    Returns
    -------
    _CoutningAttr
        An attribute that is recognized by the model.
    """
    return _define(
        footing_group=Sensitivity(),
        init=True,
        dtype=dtype,
        description=description,
        default=default,
        frozen=True,
        **kwargs,
    )


def def_parameter(
    *, dtype=None, description=None, default=None, **kwargs
) -> _CountingAttr:
    """Define a parameter to the model where a parameter is a frozen attribute that is passed on instantiation of the model.

    Parameters
    ----------
    dtype : type
        The expected type of the attribute. If not None, value will be validated on instantiation.
    description : str
        The description of the attribute.
    default : Any
        The default value of the attribute.
    kwargs : dict
        Any one of the following validators - allowed, custom, min_val, max_val, min_len, and max_len.

    Returns
    -------
    _CoutningAttr
        An attribute that is recognized by the model.
    """
    return _define(
        footing_group=Parameter(),
        init=True,
        dtype=dtype,
        description=description,
        frozen=True,
        **kwargs,
    )
