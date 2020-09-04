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
from numpydoc.docscrape import ClassDoc, FunctionDoc, Parameter

from .attributes import _Asset, _Meta, _Modifier, _Parameter
from .audit import run_model_audit
from .visualize import visualize_model


#########################################################################################
# established errors
#########################################################################################


class ModelScenarioAlreadyExist(Exception):
    """The scenario already exist."""


class ModelScenarioDoesNotExist(Exception):
    """The scenario does not exist."""


class ModelScenarioParamAlreadyExist(Exception):
    """The arugment passed to scenario already exist."""


class ModelScenarioParamDoesNotExist(Exception):
    """The parameter passed does not exist."""


class ModelRunError(Exception):
    """Error occured during model run."""


def _run(self, to_step):
    if to_step is None:
        steps = self.steps
    else:
        try:
            position = self.steps.index(to_step)
            steps = self.steps[: (position + 1)]
        except ValueError:
            msg = f"The step passed to to_step '{to_step}' does not exist as a step."
            raise ValueError(msg)

    def _run_step(step):
        try:
            return getattr(self, step)()
        except:
            exc_type, exc_value, exc_trace = sys.exc_info()
            msg = f"At step [{step}], an error occured.\n"
            msg += f"  Error Type = {exc_type}\n"
            msg += f"  Error Message = {exc_value}\n"
            msg += f"  Error Trace = {format_list(extract_tb(exc_trace))}\n"
            raise ModelRunError(msg)

    for step in steps:
        _run_step(step)

    if hasattr(self, "_return") is False:
        raise AttributeError("Missing method _return(self) for Footings object.")
    return self._return()


@attrs(slots=True, repr=False)
class Footing:

    steps: list = attrib(init=False, repr=False)
    assets: list = attrib(init=False, repr=False)

    def _return(self):
        if len(self.assets) > 1:
            return tuple(getattr(self, asset) for asset in self.assets)
        return getattr(self, self.assets[0])

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
        """
        _, file_ext = os.path.splitext(file)
        return run_model_audit(model=self, output_type=file_ext[1:], file=file, **kwargs)

    def run(self, to_step=None):
        """Run the model.

        Parameters
        ----------
        to_step : str
        """
        return _run(self, to_step=to_step)


def step(
    function: callable = None,
    *,
    uses: List[str],
    impacts: List[str],
    wrap: callable = None,
):
    """[summary]

    Parameters
    ----------
    uses : List[str]
        [description]
    impacts : List[str]
        [description]
    function : callable, optional
        [description], by default None
    wrap : callable, optional
        [description], by default None

    Returns
    -------
    [type]
        [description]
    """
    if function is None:
        return partial(step, uses=uses, impacts=impacts, wrap=wrap)

    def wrapper(*args, **kwargs):
        return function(*args, **kwargs)

    wrapper.uses = uses
    wrapper.impacts = impacts
    if wrap is not None:
        wrapper.__doc__ = wrap.__doc__
    else:
        wrapper.__doc__ = function.__doc__
    return wrapper


class FootingsFuncDoc(FunctionDoc):
    def __str__(self, func_role=""):
        out = []
        out += self._str_summary()
        out += self._str_extended_summary()
        for param_list in (
            "Parameters",
            "Returns",
            "Yields",
            "Receives",
            "Other Parameters",
            "Raises",
            "Warns",
        ):
            out += self._str_param_list(param_list)
        out += self._str_section("Warnings")
        out += self._str_see_also(func_role)
        for s in ("Notes", "References", "Examples"):
            out += self._str_section(s)
        for param_list in ("Attributes", "Methods"):
            out += self._str_param_list(param_list)
        out += self._str_index()
        return "\n".join(out)


class FootingsStepDoc(FunctionDoc):
    sections = {
        "Signature": "",
        "Summary": [""],
        "Extended Summary": [],
        "Uses": [],
        "Impacts": [],
        "Raises": [],
        "Warns": [],
        "See Also": [],
        "Notes": [],
        "Warnings": [],
        "References": "",
        "Examples": "",
        "index": {},
    }

    def _parse(self):
        self._doc.reset()
        self._parse_summary()

        sections = list(self._read_sections())
        section_names = set([section for section, content in sections])

        has_returns = "Returns" in section_names
        has_yields = "Yields" in section_names
        # We could do more tests, but we are not. Arbitrarily.
        if has_returns and has_yields:
            msg = "Docstring contains both a Returns and Yields section."
            raise ValueError(msg)
        if not has_yields and "Receives" in section_names:
            msg = "Docstring contains a Receives section but not Yields."
            raise ValueError(msg)

        for (section, content) in sections:
            if not section.startswith(".."):
                section = (s.capitalize() for s in section.split(" "))
                section = " ".join(section)
                if self.get(section):
                    self._error_location(
                        "The section %s appears twice in  %s"
                        % (section, "\n".join(self._doc._str))
                    )

            if section in ("Uses"):
                self[section] = self._parse_param_list(content)
            elif section in ("Returns", "Yields", "Raises", "Warns"):
                self[section] = self._parse_param_list(
                    content, single_element_is_type=True
                )
            elif section.startswith(".. index::"):
                self["index"] = self._parse_index(section, content)
            elif section == "See Also":
                self["See Also"] = self._parse_see_also(content)
            else:
                self[section] = content

    def __str__(self, func_role=""):
        out = []
        out += self._str_summary()
        out += self._str_extended_summary()
        for param_list in (
            "Uses",
            "Impacts",
            "Raises",
            "Warns",
        ):
            out += self._str_param_list(param_list)
        out += self._str_section("Warnings")
        out += self._str_see_also(func_role)
        for s in ("Notes", "References", "Examples"):
            out += self._str_section(s)
        out += self._str_index()
        return "\n".join(out)


class FootingsDoc(ClassDoc):
    sections = {
        "Signature": "",
        "Summary": [""],
        "Extended Summary": [],
        "Parameters": [],
        "Modifiers": [],
        "Meta": [],
        "Assets": [],
        "Steps": [],
        "Returns": [],
        "Yields": [],
        "Receives": [],
        "Raises": [],
        "Warns": [],
        "Other Parameters": [],
        "Attributes": [],
        "Methods": [],
        "See Also": [],
        "Notes": [],
        "Warnings": [],
        "References": "",
        "Examples": "",
        "index": {},
    }

    def _parse(self):
        self._doc.reset()
        self._parse_summary()

        sections = list(self._read_sections())
        section_names = set([section for section, content in sections])

        has_returns = "Returns" in section_names
        has_yields = "Yields" in section_names
        # We could do more tests, but we are not. Arbitrarily.
        if has_returns and has_yields:
            msg = "Docstring contains both a Returns and Yields section."
            raise ValueError(msg)
        if not has_yields and "Receives" in section_names:
            msg = "Docstring contains a Receives section but not Yields."
            raise ValueError(msg)

        for (section, content) in sections:
            if not section.startswith(".."):
                section = (s.capitalize() for s in section.split(" "))
                section = " ".join(section)
                if self.get(section):
                    self._error_location(
                        "The section %s appears twice in  %s"
                        % (section, "\n".join(self._doc._str))
                    )

            if section in (
                "Parameters",
                "Modifiers",
                "Meta",
                "Assets",
                "Steps",
                "Other Parameters",
                "Attributes",
                "Methods",
            ):
                self[section] = self._parse_param_list(content)
            elif section in ("Returns", "Yields", "Raises", "Warns", "Receives"):
                self[section] = self._parse_param_list(
                    content, single_element_is_type=True
                )
            elif section.startswith(".. index::"):
                self["index"] = self._parse_index(section, content)
            elif section == "See Also":
                self["See Also"] = self._parse_see_also(content)
            else:
                self[section] = content

    def __str__(self, func_role=""):
        out = []
        out += self._str_signature()
        out += self._str_summary()
        out += self._str_extended_summary()
        for param_list in (
            "Parameters",
            "Modifiers",
            "Meta",
            "Assets",
            "Steps",
            "Returns",
            "Yields",
            "Receives",
            "Other Parameters",
            "Raises",
            "Warns",
        ):
            out += self._str_param_list(param_list)
        out += self._str_section("Warnings")
        out += self._str_see_also(func_role)
        for s in ("Notes", "References", "Examples"):
            out += self._str_section(s)
        for param_list in ("Methods",):
            out += self._str_param_list(param_list)
        out += self._str_index()
        return "\n".join(out)


_FOOTING_GROUP_MAP = {
    _Asset: "Assets",
    _Meta: "Meta",
    _Modifier: "Modifiers",
    _Parameter: "Parameters",
}


def _update_run_return(cls, assets):
    run_doc = FootingsFuncDoc(cls.run)

    if cls._return.__doc__ is not None:
        return_doc = FootingsFuncDoc(cls._return)
        run_doc["Returns"] = return_doc["Returns"]
    else:
        run_doc["Returns"] = assets

    return str(run_doc)


def _attr_doc(cls, steps):
    def add_doc(x):
        str_type = str(x.type) if x.type is not None else ""
        return Parameter(x.name, str_type, [x.metadata.get("description", "")])

    sections = ["Parameters", "Modifiers", "Meta", "Assets"]
    parsed_meta = {section: [] for section in sections}

    for attribute in cls.__attrs_attrs__:
        section = _FOOTING_GROUP_MAP.get(attribute.metadata.get("footing_group"), None)
        if section is not None:
            if section not in sections:
                msg = f"The footing_group {section} is not one of the known groups [{str(sections)}]."
                warnings.warn(msg)
            else:
                parsed_meta[section].append(add_doc(attribute))

    cls.run.__doc__ = _update_run_return(cls, parsed_meta["Assets"])

    doc = FootingsDoc(cls)

    if len(parsed_meta["Parameters"]) > 0:
        doc["Parameters"] = parsed_meta["Parameters"]
    if len(parsed_meta["Modifiers"]) > 0:
        doc["Modifiers"] = parsed_meta["Modifiers"]
    if len(parsed_meta["Meta"]) > 0:
        doc["Meta"] = parsed_meta["Meta"]
    if len(parsed_meta["Assets"]) > 0:
        doc["Assets"] = parsed_meta["Assets"]
    if len(steps) > 0:

        attributes = {
            attribute.name: add_doc(attribute) for attribute in cls.__attrs_attrs__
        }

        def add_step_summary(step_func, attributes):
            doc = FootingsStepDoc(step_func)
            doc["Uses"] = [attributes[x] for x in step_func.uses]
            doc["Impacts"] = [attributes[x] for x in step_func.impacts]
            return str(doc)

        doc["Steps"] = [
            Parameter(step, "", [add_step_summary(getattr(cls, step), attributes)])
            for step in steps
        ]
    cls.__doc__ = str(doc)
    return cls


def model(steps: List[str] = None):
    """
    """

    def inner(cls):
        # test if is subclass of Footing
        if issubclass(cls, Footing) is False:
            msg = "The object is not a child of the Footing class."
            raise TypeError(msg)

        # analyze attributes
        exclude = [x for x in dir(Footing) if x[0] != "_"]
        attributes = [x for x in dir(cls) if x[0] != "_" and x not in exclude]
        assets = []
        for attribute in attributes:
            attr = getattr(cls, attribute)
            if isinstance(attr, _CountingAttr) is False:
                msg = f"The attribute {attribute} is not registered to a known Footings group.\n"
                msg += "Use one of define_parameter, define_meta, define_modifier or define_asset "
                msg += "when building a model."
                raise AttributeError(msg)
            footing_group = attr.metadata.get("footing_group", None)
            if footing_group is None:
                msg = f"The attribute {attribute} is not registered to a known Footings group.\n"
                msg += "Use one of define_parameter, define_meta, define_modifier or define_asset "
                msg += "when building a model."
                raise AttributeError(msg)
            if footing_group is _Asset:
                assets.append(attribute)

        # make sure at least one asset
        if len(assets) == 0:
            msg = "No assets registered to model. At least one asset needs to be registered."
            raise AttributeError(msg)

        # analyze steps
        missing = []
        for step in steps:
            if getattr(cls, step, None) is None:
                missing.append(step)
        if len(missing) > 0:
            msg = f"The following steps listed are missing - {str(missing)} from the object."
            raise AttributeError(msg)

        # update steps and assets with known values as defaults
        cls.steps = attrib(default=steps, init=False, repr=False)
        cls.assets = attrib(default=assets, init=False, repr=False)

        cls = attrs(
            maybe_cls=cls, kw_only=True, on_setattr=frozen, repr=False, slots=True
        )
        old_sig = signature(cls)
        new_sig = old_sig.replace(return_annotation=f"{cls.__name__}")
        cls.__signature__ = new_sig
        return _attr_doc(cls, steps)

    return inner
