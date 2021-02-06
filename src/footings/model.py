import sys
from enum import Enum, auto
from functools import partial
from traceback import extract_tb, format_list
from typing import Any, List, Optional

import numpydoc.docscrape as numpydoc
from attr import NOTHING, attrib, attrs, evolve, make_class
from attr._make import _CountingAttr
from attr.setters import NO_OP, frozen

from .audit import run_model_audit
from .doc_tools.docscrape import FootingsDoc
from .exceptions import ModelCreationError, ModelRunError
from .visualize import visualize_model

__all__ = [
    "Model",
    "model",
    "step",
    "def_parameter",
    "def_sensitivity",
    "def_meta",
    "def_intermediate",
    "def_return",
]


class ModelAttributeType(Enum):
    """Allowed dodel attribute types belonging to a model."""

    Parameter = auto()
    Sensitivity = auto()
    Meta = auto()
    Intermediate = auto()
    Return = auto()


def _define(
    attribute_type: ModelAttributeType,
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
        attribute_type=ModelAttributeType.Parameter,
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
        attribute_type=ModelAttributeType.Sensitivity,
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
        attribute_type=ModelAttributeType.Meta,
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
        attribute_type=ModelAttributeType.Intermediate,
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
        attribute_type=ModelAttributeType.Return,
        init=False,
        dtype=dtype,
        description=description,
        default=init_value,
        frozen=False,
        **kwargs,
    )


def _run(self, to_step):
    if len(self.__model_steps__) == 0:
        raise ModelRunError("Not able to run model because no steps are registered.")
    if len(self.__model_returns__) == 0:
        raise ModelRunError(
            "Not able to run model because no return attributes are registered."
        )
    if to_step is None:
        steps = self.__model_steps__
    else:
        try:
            position = self.__model_steps__.index(to_step)
            steps = self.__model_steps__[: (position + 1)]
        except ValueError as e:
            msg = f"The step passed to to_step '{to_step}' does not exist as a step."
            raise e(msg)

    def _run_step(step):
        try:
            return getattr(self, step)()
        except:
            exc_type, exc_value, exc_trace = sys.exc_info()
            msg = f"At step [{step}], an error occured.\n"
            msg += f"  Error Type = {exc_type.__name__}\n"
            msg += f"  Error Message = {exc_value}\n"
            msg += f"  Error Trace = {format_list(extract_tb(exc_trace))}\n"
            raise ModelRunError(msg)

    for step in steps:
        _run_step(step)

    if to_step is not None:
        return self
    if len(self.__model_returns__) > 1:
        return tuple(getattr(self, ret) for ret in self.__model_returns__)
    return getattr(self, self.__model_returns__[0])


@attrs(slots=True, repr=False)
class Model:
    """The parent modeling class providing the key methods of run, audit, and visualize."""

    __model_steps__: tuple = attrib(init=False, repr=False)
    __model_parameters__: tuple = attrib(init=False, repr=False)
    __model_sensitivities__: tuple = attrib(init=False, repr=False)
    __model_meta__: tuple = attrib(init=False, repr=False)
    __model_intermediates__: tuple = attrib(init=False, repr=False)
    __model_returns__: tuple = attrib(init=False, repr=False)
    __model_attribute_map__: dict = attrib(init=False, repr=False)

    def visualize(self):
        """Visualize the model to get an understanding of what model attributes are used and when."""
        return visualize_model(self)

    def audit(self, file: str = None, **kwargs):
        """Audit the model which returns copies of the object as it is modified across each step.

        Parameters
        ----------
        file : str, optional
            The name of the audit output file.
        kwargs
            Additional key words passed to audit.

        Returns
        -------
        None
            An audit file in specfified format (e.g., .xlsx).
        """
        return run_model_audit(model=self, file=file, **kwargs)

    def run(self, to_step=None):
        """Runs the model and returns any returns defined.

        Parameters
        ----------
        to_step : str, optional
            The name of the step to run model to.

        """
        return _run(self, to_step=to_step)


@attrs(frozen=True, slots=True)
class Step:
    name = attrib(type=str)
    docstring = attrib(type=str)
    method = attrib(type=callable)
    method_name = attrib(type=str)
    uses = attrib(type=List[str])
    impacts = attrib(type=List[str])
    metadata = attrib(type=dict, factory=dict)

    @classmethod
    def create(
        cls,
        method: callable,
        uses: List[str],
        impacts: List[str],
        name: Optional[str] = None,
        docstring: Optional[str] = None,
        metadata: Optional[dict] = None,
    ):
        method_name = method.__qualname__.split(".")[1]
        return cls(
            name=name if name is not None else method_name,
            docstring=docstring if docstring is not None else method.__doc__,
            method=method,
            method_name=method_name,
            uses=uses,
            impacts=impacts,
            metadata={} if metadata is None else metadata,
        )

    def __doc__(self):
        return self.docstring

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        return partial(self, obj)

    def __call__(self, *args, **kwargs):
        return self.method(*args, **kwargs)


def step(
    method: callable = None,
    *,
    uses: List[str],
    impacts: List[str],
    name: str = None,
    docstring: str = None,
    metadata: dict = None,
):
    """Turn a method into a step within the footings framework.

    Parameters
    ----------
    method : callable, optional
        The method to decorate, by default None.
    uses : List[str]
        A list of the object names used by the step.
    impacts : List[str]
        A list of the object names that are impacted by the step (i.e., the returns and intermediates).
    wrap : callable, optional
        Wrap or source the docstring from another object, by default None.

    Returns
    -------
    callable
        The decorated method with a attributes for uses and impacts and updated docstring if wrap passed.
    """
    if method is None:
        return partial(
            step,
            uses=uses,
            impacts=impacts,
            name=name,
            docstring=docstring,
            metadata=metadata,
        )

    return Step.create(
        method=method,
        uses=uses,
        impacts=impacts,
        name=name,
        docstring=docstring,
        metadata=metadata,
    )


def _make_doc_parameter(attribute):
    if attribute.type is None:
        atype = ""
    elif isinstance(attribute.type, str):
        atype = attribute.type
    elif hasattr(attribute.type, "__qualname__"):
        atype = attribute.type.__qualname__
    else:
        try:
            atype = str(attribute.type)
        except:
            atype = ""
    return numpydoc.Parameter(
        attribute.name, atype, [attribute.metadata.get("description", "")]
    )


def _parse_attriubtes(cls):
    sections = ["Parameters", "Sensitivities", "Meta", "Intermediates", "Returns"]
    parsed_attributes = {section: [] for section in sections}

    for attribute in cls.__attrs_attrs__:
        attribute_type = attribute.metadata.get("footings_attribute_type", None)
        if attribute_type is ModelAttributeType.Parameter:
            parsed_attributes["Parameters"].append(_make_doc_parameter(attribute))
        elif attribute_type is ModelAttributeType.Sensitivity:
            parsed_attributes["Sensitivities"].append(_make_doc_parameter(attribute))
        elif attribute_type is ModelAttributeType.Meta:
            parsed_attributes["Meta"].append(_make_doc_parameter(attribute))
        elif attribute_type is ModelAttributeType.Intermediate:
            parsed_attributes["Intermediates"].append(_make_doc_parameter(attribute))
        elif attribute_type is ModelAttributeType.Return:
            parsed_attributes["Returns"].append(_make_doc_parameter(attribute))

    return parsed_attributes


def _generate_steps_sections(cls, steps):
    return [
        f"{idx}) {getattr(cls, step).name} - {getattr(cls, step).docstring}"
        for idx, step in enumerate(steps, start=1)
    ]


def _attr_doc(cls, steps):

    parsed_attributes = _parse_attriubtes(cls)
    doc = FootingsDoc(cls)

    doc["Parameters"] = parsed_attributes["Parameters"]
    doc["Sensitivities"] = parsed_attributes["Sensitivities"]
    doc["Meta"] = parsed_attributes["Meta"]
    doc["Intermediates"] = parsed_attributes["Intermediates"]
    doc["Returns"] = parsed_attributes["Returns"]
    doc["Steps"] = _generate_steps_sections(cls, steps)
    doc["Methods"] = []

    cls.__doc__ = str(doc)
    return cls


def _update_uses_impacts(src, attribute_map):
    flags = ["parameter.", "sensitivity.", "meta.", "intermediate.", "return."]

    def inner(x):
        if any([flag in x for flag in flags]):
            return x
        if x not in attribute_map:
            msg = f"The attribute [{x}] does not belong to the model."
            raise ModelCreationError(msg)
        return attribute_map[x]

    return tuple([inner(x) for x in src])


def model(cls: type = None, *, steps: List[str] = []):
    """Turn a class into a model within the footings framework.

    Parameters
    ----------
    cls : type
        The class to turn into a model.
    steps : List[str], optional
        The list of steps to the model.

    Returns
    -------
    cls
        Returns cls as a model within footings framework.
    """
    if cls is None:
        return partial(model, steps=steps)

    def inner(cls):
        # In order to be instantiated as a model, need to pass the following test.

        # 1. All attributes need to belong to a footings_attribute_type
        exclude = ["run", "audit", "visualize"]
        attributes = [x for x in cls.__dict__.keys() if x[0] != "_" and x not in exclude]
        if hasattr(cls, "__attrs_attrs__"):
            attributes += [x.name for x in cls.__attrs_attrs__ if x.name[0] != "_"]
            attrs_attrs = {x.name: x for x in cls.__attrs_attrs__ if x.name[0] != "_"}
        else:
            attrs_attrs = {}
        parameters, sensitivities, meta, intermediates, returns = [], [], [], [], []
        for attribute in attributes:
            attr = getattr(cls, attribute)
            if attribute in attrs_attrs:
                attr = attrs_attrs[attribute]
            else:
                if isinstance(attr, _CountingAttr) is False:
                    msg = f"The attribute {attribute} is not registered to a known Models group.\n"
                    msg += "Use one of def_* functions from the footings library when building a model."
                    raise ModelCreationError(msg)
            attribute_type = attr.metadata.get("footings_attribute_type", None)
            if (
                attribute_type is None
                or isinstance(attribute_type, ModelAttributeType) is False
            ):
                msg = f"The attribute {attribute} is not registered to a known Models attribute type.\n"
                msg += "Use one of def_* functions from the footings library when building a model."
                raise ModelCreationError(msg)
            if attribute_type is ModelAttributeType.Parameter:
                parameters.append(attribute)
            elif attribute_type is ModelAttributeType.Sensitivity:
                sensitivities.append(attribute)
            elif attribute_type is ModelAttributeType.Meta:
                meta.append(attribute)
            elif attribute_type is ModelAttributeType.Intermediate:
                intermediates.append(attribute)
            elif attribute_type is ModelAttributeType.Return:
                returns.append(attribute)

        # 2. For steps -
        #    - all steps are methods of cls
        #    - all steps have attributes uses and impacts
        missing_steps = []
        missing_attributes = []
        for step_nm in steps:
            step = getattr(cls, step_nm, None)
            if step is None:
                missing_steps.append(step_nm)
            if hasattr(step, "uses") is False or hasattr(step, "impacts") is False:
                missing_attributes.append(step_nm)
        if len(missing_steps) > 0:
            raise ModelCreationError(
                f"The following steps listed are missing - {str(missing_steps)} from the object."
            )
        if len(missing_attributes) > 0:
            raise ModelCreationError(
                f"The followings steps listed do not appear to be decorated steps - {str(missing_attributes)}."
            )

        # If all test pass, update steps and returns with known values as defaults.
        cls.__model_steps__ = tuple(steps)
        cls.__model_parameters__ = tuple(parameters)
        cls.__model_sensitivities__ = tuple(sensitivities)
        cls.__model_meta__ = tuple(meta)
        cls.__model_intermediates__ = tuple(intermediates)
        cls.__model_returns__ = tuple(returns)

        attribute_map = {
            **{p: f"parameter.{p}" for p in parameters},
            **{m: f"sensitivity.{m}" for m in sensitivities},
            **{m: f"meta.{m}" for m in meta},
            **{p: f"intermediate.{p}" for p in intermediates},
            **{a: f"return.{a}" for a in returns},
        }

        for step in steps:
            use_old = getattr(cls, step).uses
            use_new = _update_uses_impacts(use_old, attribute_map)
            impact_old = getattr(cls, step).impacts
            impact_new = _update_uses_impacts(impact_old, attribute_map)
            new_step = evolve(getattr(cls, step), uses=use_new, impacts=tuple(impact_new))
            setattr(cls, step, new_step)
        cls.__model_attribute_map__ = attribute_map

        exclude = [x for x in Model.__dict__.keys() if x[0] != "_"]
        attrs = {
            x: getattr(cls, x)
            for x in cls.__dict__.keys()
            if x[0] != "_" and x not in exclude
        }

        # Make attrs dataclass and update signature
        cls = make_class(
            cls.__name__,
            attrs=attrs,
            bases=(cls, Model,),
            kw_only=True,
            on_setattr=frozen,
            repr=False,
            slots=True,
        )
        return _attr_doc(cls, steps)

    return inner(cls)
