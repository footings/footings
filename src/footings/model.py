from functools import partial
from inspect import signature
import sys
from traceback import extract_tb, format_list
from typing import List, Optional

from attr import attrs, attrib, evolve
from attr._make import _CountingAttr
from attr.setters import frozen
import numpydoc.docscrape as numpydoc

from .attributes import Parameter, Sensitivity, Meta, Intermediate, Return
from .audit import run_model_audit
from .doc_tools.docscrape import FootingsDoc
from .visualize import visualize_model


class ModelRunError(Exception):
    """Error occured during model run."""


class ModelCreationError(Exception):
    """Error occured creating model object."""


def _run(self, to_step):

    if to_step is None:
        steps = self.__footings_steps__
    else:
        try:
            position = self.__footings_steps__.index(to_step)
            steps = self.__footings_steps__[: (position + 1)]
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
    if len(self.__footings_returns__) > 1:
        return tuple(getattr(self, ret) for ret in self.__footings_returns__)
    return getattr(self, self.__footings_returns__[0])


@attrs(slots=True, repr=False)
class Footing:
    """The parent modeling class providing the key methods of run, audit, and visualize."""

    __footings_steps__: tuple = attrib(init=False, repr=False)
    __footings_parameters__: tuple = attrib(init=False, repr=False)
    __footings_sensitivities__: tuple = attrib(init=False, repr=False)
    __footings_meta__: tuple = attrib(init=False, repr=False)
    __footings_intermediates__: tuple = attrib(init=False, repr=False)
    __footings_returns__: tuple = attrib(init=False, repr=False)
    __footings_attribute_map__: dict = attrib(init=False, repr=False)

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
        grp = attribute.metadata.get("footing_group", None)
        if isinstance(grp, Parameter):
            parsed_attributes["Parameters"].append(_make_doc_parameter(attribute))
        elif isinstance(grp, Sensitivity):
            parsed_attributes["Sensitivities"].append(_make_doc_parameter(attribute))
        elif isinstance(grp, Meta):
            parsed_attributes["Meta"].append(_make_doc_parameter(attribute))
        elif isinstance(grp, Intermediate):
            parsed_attributes["Intermediates"].append(_make_doc_parameter(attribute))
        elif isinstance(grp, Return):
            parsed_attributes["Returns"].append(_make_doc_parameter(attribute))

    return parsed_attributes


def _generate_steps_sections(cls, steps):
    return [
        f"{idx}) {step} - {getattr(cls, step).docstring}"
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


def _prepare_signature(cls):
    old_sig = signature(cls)
    return old_sig.replace(return_annotation=f"{cls.__name__}")


def model(cls: type = None, *, steps: List[str]):
    """Turn a class into a model within the footings framework.

    Parameters
    ----------
    cls : type
        The class to turn into a model.
    steps : List[str]
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

        # 1. All attributes need to belong to a footings_group
        exclude = ["run", "audit", "visualize"]
        attributes = [x for x in cls.__dict__.keys() if x[0] != "_" and x not in exclude]
        if hasattr(cls, "__attrs_attrs__"):
            attrs_attrs = {x.name: x for x in cls.__attrs_attrs__}
        else:
            attrs_attrs = {}
        parameters, sensitivities, meta, intermediates, returns = [], [], [], [], []
        for attribute in attributes:
            attr = getattr(cls, attribute)
            if attribute in attrs_attrs:
                attr = attrs_attrs[attribute]
            else:
                if isinstance(attr, _CountingAttr) is False:
                    msg = f"The attribute {attribute} is not registered to a known Footings group.\n"
                    msg += "Use one of def_parameter, def_meta, def_sensitivity, def_intermediate "
                    msg += "or def_return when building a model."
                    raise ModelCreationError(msg)
            footing_group = attr.metadata.get("footing_group", None)
            if footing_group is None:
                msg = f"The attribute {attribute} is not registered to a known Footings group.\n"
                msg += (
                    "Use one of def_parameter, def_meta, def_sensitivity or def_return "
                )
                msg += "when building a model."
                raise ModelCreationError(msg)
            if isinstance(footing_group, Parameter):
                parameters.append(attribute)
            elif isinstance(footing_group, Sensitivity):
                sensitivities.append(attribute)
            elif isinstance(footing_group, Meta):
                meta.append(attribute)
            elif isinstance(footing_group, Intermediate):
                intermediates.append(attribute)
            elif isinstance(footing_group, Return):
                returns.append(attribute)

        # 2. Make sure at least one return
        if len(returns) == 0:
            raise ModelCreationError(
                "No returns registered to model. At least one ret needs to be registered."
            )

        # 3. For steps -
        #    - make sure at least one step in model
        #    - all steps are methods of cls
        #    - all steps have attributes uses and impacts
        if len(steps) == 0:
            raise ModelCreationError("Model needs to have at least one step.")
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

        # Add Footings methods
        cls.run = Footing.run
        cls.audit = Footing.audit
        cls.visualize = Footing.visualize

        # If all test pass, update steps and returns with known values as defaults.
        kws = {"init": False, "repr": False}
        cls.__footings_steps__ = attrib(default=tuple(steps), **kws)
        cls.__footings_parameters__ = attrib(default=tuple(parameters), **kws)
        cls.__footings_sensitivities__ = attrib(default=tuple(sensitivities), **kws)
        cls.__footings_meta__ = attrib(default=tuple(meta), **kws)
        cls.__footings_intermediates__ = attrib(default=tuple(intermediates), **kws)
        cls.__footings_returns__ = attrib(default=tuple(returns), **kws)

        attribute_map = {
            **{p: f"parameter.{p}" for p in parameters},
            **{m: f"sensitivity.{m}" for m in sensitivities},
            **{m: f"meta.{m}" for m in meta},
            **{p: f"intermediate.{p}" for p in intermediates},
            **{a: f"return.{a}" for a in returns},
        }
        for step in steps:
            use_old = getattr(cls, step).uses
            use_new = [attribute_map[x] for x in use_old]
            impact_old = getattr(cls, step).impacts
            impact_new = [attribute_map[x] for x in impact_old]
            setattr(
                cls, step, evolve(getattr(cls, step), uses=use_new, impacts=impact_new)
            )
        cls.__footings_attribute_map__ = attrib(default=attribute_map, **kws)

        # Make attrs dataclass and update signature
        cls = attrs(
            maybe_cls=cls, kw_only=True, on_setattr=frozen, repr=False, slots=True
        )
        cls.__signature__ = _prepare_signature(cls)
        return _attr_doc(cls, steps)

    return inner(cls)
