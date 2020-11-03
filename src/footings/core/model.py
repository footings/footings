from functools import partial
from inspect import signature
import os
import sys
from traceback import extract_tb, format_list
from typing import List
import warnings

from attr import attrs, attrib
from attr._make import _CountingAttr
from attr.setters import frozen
from numpydoc.docscrape import Parameter, FunctionDoc

from .attributes import _Parameter, _Modifier, _Meta, _Placeholder, _Asset
from .audit import run_model_audit
from ..doc_tools.docscrape import FootingsDoc
from .visualize import visualize_model


class ModelRunError(Exception):
    """Error occured during model run."""


class ModelCreationError(Exception):
    """Error occured creating model object."""


def _run(self, to_step):

    if to_step is None:
        steps = self.steps
    else:
        try:
            position = self.steps.index(to_step)
            steps = self.steps[: (position + 1)]
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
    if len(self.assets) > 1:
        return tuple(getattr(self, asset) for asset in self.assets)
    return getattr(self, self.assets[0])


@attrs(slots=True, repr=False)
class Footing:
    """The parent modeling class providing the key methods of run, audit, and visualize."""

    steps: list = attrib(init=False, repr=False)
    assets: list = attrib(init=False, repr=False)

    def visualize(self):
        """Visualize the model to get an understanding of what model attributes are used and when."""
        return visualize_model(self)

    def audit(self, file: str, **kwargs):
        """Audit the model which returns copies of the object as it is modified across each step.

        Parameters
        ----------
        file : str
            The name of the audit file.
        kwargs
            Additional key words passed to audit.

        Returns
        -------
        None
            An audit file in specfified format (e.g., .xlsx).
        """
        _, file_ext = os.path.splitext(file)
        return run_model_audit(model=self, output_type=file_ext[1:], file=file, **kwargs)

    def run(self, to_step=None):
        """Runs the model and returns any assets defined.

        Parameters
        ----------
        to_step : str
            The name of the step to run model to.

        """
        return _run(self, to_step=to_step)


def step(
    method: callable = None,
    *,
    uses: List[str],
    impacts: List[str],
    wrap: callable = None,
):
    """Turn a method into a step within the footings framework.

    Parameters
    ----------
    method : callable, optional
        The method to decorate, by default None.
    uses : List[str]
        A list of the object names used by the step.
    impacts : List[str]
        A list of the object names that are impacted by the step (i.e., the assets and placeholders).
    wrap : callable, optional
        Wrap or source the docstring from another object, by default None.

    Returns
    -------
    callable
        The decorated method with a attributes for uses and impacts and updated docstring if wrap passed.
    """
    if method is None:
        return partial(step, uses=uses, impacts=impacts, wrap=wrap)

    def wrapper(*args, **kwargs):
        return method(*args, **kwargs)

    wrapper.uses = uses
    wrapper.impacts = impacts
    if wrap is not None:
        wrapper.__doc__ = wrap.__doc__
    else:
        wrapper.__doc__ = method.__doc__
    return wrapper


_FOOTING_GROUP_MAP = {
    _Parameter: "Parameters",
    _Modifier: "Modifiers",
    _Meta: "Meta",
    _Placeholder: "Placeholders",
    _Asset: "Assets",
}


def _parse_attriubtes(cls):
    sections = ["Parameters", "Modifiers", "Meta", "Placeholders", "Assets"]
    parsed_attributes = {section: [] for section in sections}

    for attribute in cls.__attrs_attrs__:
        section = _FOOTING_GROUP_MAP.get(attribute.metadata.get("footing_group"), None)
        if section is not None:
            if section not in sections:
                msg = f"The footing_group {section} is not one of the known groups [{str(sections)}]."
                warnings.warn(msg)
            else:
                parsed_attributes[section].append(_make_doc_parameter(attribute))
    return parsed_attributes


def _make_doc_parameter(attribute):
    str_type = str(attribute.type) if attribute.type is not None else ""
    return Parameter(
        attribute.name, str_type, [attribute.metadata.get("description", "")]
    )


def _update_run_return(cls, assets):
    run_doc = FunctionDoc(cls.run)

    if cls._return.__doc__ is not None:
        return_doc = FunctionDoc(cls._return)
        run_doc["Returns"] = return_doc["Returns"]
    else:
        run_doc["Returns"] = assets

    return str(run_doc)


def _generate_steps_sections(cls, steps):
    def add_step_summary(step_func):
        doc = FunctionDoc(step_func)
        return "\n".join(doc["Summary"])

    return [
        f"{idx}. {step} - {add_step_summary(getattr(cls, step))}"
        for idx, step in enumerate(steps, start=1)
    ]


def _attr_doc(cls, steps):

    parsed_attributes = _parse_attriubtes(cls)

    doc = FootingsDoc(cls)

    doc["Parameters"] = parsed_attributes["Parameters"]
    doc["Modifiers"] = parsed_attributes["Modifiers"]
    doc["Meta"] = parsed_attributes["Meta"]
    doc["Placeholders"] = parsed_attributes["Placeholders"]
    doc["Assets"] = parsed_attributes["Assets"]
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

        # 1. Needs to be a subclass of Footing
        if issubclass(cls, Footing) is False:
            raise ModelCreationError(
                f"The object {str(cls)} is not a child of the Footing class."
            )

        # 2. All attributes need to belong to a footings_group
        exclude = [x for x in dir(Footing) if x[0] != "_"]
        attributes = [x for x in dir(cls) if x[0] != "_" and x not in exclude]
        if hasattr(cls, "__attrs_attrs__"):
            attrs_attrs = {x.name: x for x in cls.__attrs_attrs__}
        assets = []
        for attribute in attributes:
            attr = getattr(cls, attribute)
            if attribute in attrs_attrs:
                attr = attrs_attrs[attribute]
            else:
                if isinstance(attr, _CountingAttr) is False:
                    msg = f"The attribute {attribute} is not registered to a known Footings group.\n"
                    msg += "Use one of define_parameter, define_meta, define_modifier, define_placeholder "
                    msg += "or define_asset when building a model."
                    raise ModelCreationError(msg)
            footing_group = attr.metadata.get("footing_group", None)
            if footing_group is None:
                msg = f"The attribute {attribute} is not registered to a known Footings group.\n"
                msg += "Use one of define_parameter, define_meta, define_modifier or define_asset "
                msg += "when building a model."
                raise ModelCreationError(msg)
            if footing_group is _Asset:
                assets.append(attribute)

        # 3. Make sure at least one asset
        if len(assets) == 0:
            raise ModelCreationError(
                "No assets registered to model. At least one asset needs to be registered."
            )

        # 4. For steps -
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

        # If all test pass, update steps and assets with known values as defaults.
        cls.steps = attrib(default=steps, init=False, repr=False)
        cls.assets = attrib(default=assets, init=False, repr=False)

        # Make attrs dataclass and update signature
        cls = attrs(
            maybe_cls=cls, kw_only=True, on_setattr=frozen, repr=False, slots=True
        )
        cls.__signature__ = _prepare_signature(cls)
        return _attr_doc(cls, steps)

    return inner(cls)
