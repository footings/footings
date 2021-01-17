from enum import Enum, auto
from typing import Any, Optional

from attr import attrib, NOTHING
from attr.setters import NO_OP


class FootingsAttributeType(Enum):
    """"""

    Parameter = auto()
    Sensitivity = auto()
    Meta = auto()
    Intermediate = auto()
    Return = auto()


def _define(
    attribute_type: FootingsAttributeType,
    init: bool,
    frozen: bool,
    dtype: Optional[Any] = None,
    description: Optional[str] = None,
    default: Optional[Any] = NOTHING,
    validator: Optional[callable] = None,
    converter: Optional[callable] = None,
    **kwargs,
):
    metadata = {
        "description": description if description is not None else "",
        "footings_attribute_type": attribute_type,
    }
    on_setattr = NO_OP if frozen is False else None

    return attrib(
        init=init,
        type=dtype,
        repr=False,
        default=default,
        validator=validator,
        converter=converter,
        metadata=metadata,
        on_setattr=on_setattr,
        kw_only=True,
        **kwargs,
    )


def def_parameter(
    *,
    dtype: Optional[Any] = None,
    description: Optional[str] = None,
    converter: Optional[callable] = None,
    validator: Optional[callable] = None,
    **kwargs,
):
    """Define a parameter attribute under the model.

    A parameter attribute is a frozen attribute that is passed on instantiation of the model.

    :param Optional[Any] dtype: The expected type of the attribute. If not None, value will be
        validated on instantiation.
    :param Optional[str] description: Optional description to add.
    :param Optional[callable] converter: Optional callable that is used to convert value to desired format.
    :param Optional[callable] validator: Optional callaable that is used to validate value.
    :param kwargs: Advanced options to pass through to `attrs.attrib`.
    """
    return _define(
        attribute_type=FootingsAttributeType.Parameter,
        init=True,
        dtype=dtype,
        description=description,
        converter=converter,
        validator=validator,
        frozen=True,
        **kwargs,
    )


def def_sensitivity(
    *,
    default: Any = NOTHING,
    dtype: Optional[Any] = None,
    description: Optional[str] = None,
    converter: Optional[callable] = None,
    validator: Optional[callable] = None,
    **kwargs,
):
    """Define a sensitivity attribute under the model.

    A sensitivity attribute is a frozen attribute with a default value that is passed on
    instantiation of the model.

    :param Any default: The default value of the sensitivity.
    :param Optional[Any] dtype: The expected type of the attribute. If not None, value will be
        validated on instantiation.
    :param Optional[str] description: Optional description to add.
    :param Optional[callable] converter: Optional callable that is used to convert value to desired format.
    :param Optional[callable] validator: Optional callaable that is used to validate value.
    :param kwargs: Advanced options to pass through to `attrs.attrib`.
    """
    return _define(
        attribute_type=FootingsAttributeType.Sensitivity,
        init=True,
        dtype=dtype,
        description=description,
        default=default,
        converter=converter,
        validator=validator,
        frozen=True,
        **kwargs,
    )


def def_meta(
    *,
    meta: Any,
    dtype: Optional[Any] = None,
    description: Optional[str] = None,
    converter: Optional[callable] = None,
    validator: Optional[callable] = None,
    **kwargs,
):
    """Define a meta attribute under the model.

    A meta attribute is a frozen attribute that is passed on instantiation of the model.

    :param Any meta: The meta value to pass to the model.
    :param Optional[Any] dtype: The expected type of the attribute. If not None, value will be
        validated on instantiation.
    :param Optional[str] description: Optional description to add.
    :param Optional[callable] converter: Optional callable that is used to convert value to desired format.
    :param Optional[callable] validator: Optional callaable that is used to validate value.
    :param kwargs: Advanced options to pass through to `attrs.attrib`.
    """
    return _define(
        attribute_type=FootingsAttributeType.Meta,
        init=False,
        dtype=dtype,
        description=description,
        default=meta,
        converter=converter,
        validator=validator,
        frozen=True,
        **kwargs,
    )


def def_intermediate(
    *,
    dtype: Optional[Any] = None,
    description: Optional[str] = None,
    init_value: Optional[Any] = None,
    **kwargs,
):
    """Define an intermediate attribute under the model.

    A placeholder is a non-frozen attribute that is created by the model and not returned when
    the model runs. It can be used to hold intermediate values for calculation.

    :param Optional[Any] dtype: The expected type of the attribute. If not None, value will be
        validated on instantiation.
    :param Optional[str] description: Optional description to add.
    :param Optional[Any] init_value: Optional initival value to assign.
    :param kwargs: Advanced options to pass through to `attrs.attrib`.
    """
    return _define(
        attribute_type=FootingsAttributeType.Intermediate,
        init=False,
        dtype=dtype,
        description=description,
        default=init_value,
        frozen=False,
        **kwargs,
    )


def def_return(
    *,
    dtype: Optional[Any] = None,
    description: Optional[str] = None,
    init_value: Optional[Any] = None,
    **kwargs,
):
    """Define an intermediate attribute under the model.

    A placeholder is a non-frozen attribute that is created by the model and returned when
    the model runs.

    :param Optional[Any] dtype: The expected type of the attribute. If not None, value will be
        validated on instantiation.
    :param Optional[str] description: Optional description to add.
    :param Optional[Any] init_value: Optional initival value to assign.
    :param kwargs: Advanced options to pass through to `attrs.attrib`.
    """
    return _define(
        attribute_type=FootingsAttributeType.Return,
        init=False,
        dtype=dtype,
        description=description,
        default=init_value,
        frozen=False,
        **kwargs,
    )
