from typing import List, Dict
from inspect import getfullargspec
import sys

from attr import attrs, attrib, make_class
from numpydoc.docscrape import FunctionDoc

from .footing import create_footing_from_list
from .audit import run_model_audit
from .visualize import visualize_model

__all__ = ["build_model"]

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


#########################################################################################
# model
#########################################################################################


def create_dependency_index(dependencies):
    """Create dependency index"""
    store = set()
    store_dict = {}
    keys = list(dependencies.keys())
    keys_reverse = keys[::-1]
    for key in keys_reverse:
        if dependencies[key] != set():
            store.update(dependencies[key])
        store_dict.update({key: store.copy()})

    ret = {
        prior: store_dict[current]
        for prior, current in zip(keys_reverse[1:][::-1], keys_reverse[:-1][::-1])
    }
    ret.update({keys[-1]: set([keys[-1]])})

    ret = {
        v[0]: v[1].intersection(set(keys[: (idx + 1)]))
        for idx, v in enumerate(ret.items())
    }
    return ret


def update_dict_(dict_, dependency_index, step, output):
    """Update store"""
    dict_.update({step: output})
    keys = list(dict_.keys())
    for key in keys:
        if key not in dependency_index:
            del dict_[key]


def run_model(model):
    """Generic function to run a model"""
    if not issubclass(type(model), BaseModel):
        raise TypeError(f"The model passed must be a subclass of {BaseModel}.")

    dict_ = {}
    steps = model.steps
    dependency_index = model.dependency_index
    for k, v in steps.items():
        if k not in dependency_index[k]:
            continue
        init_params = {k: getattr(model, v) for k, v in v.init_params.items()}
        dependent_params = {
            k: v.get_value(dict_[v.name]) for k, v in v.dependent_params.items()
        }
        try:
            out = v.function(**init_params, **dependent_params, **v.defined_params)
        except:
            exc_type, exc_value, _ = sys.exc_info()
            msg = f"At step [{k}], an error occured.\n"
            msg += f"  Error Type = {exc_type}\n"
            msg += f"  Error Message = {exc_value}"
            raise ModelRunError(msg)
        update_dict_(dict_, dependency_index[k], k, out)
    return dict_[list(steps.keys())[-1]]


@attrs(slots=True, frozen=True, repr=False)
class BaseModel:
    """BaseModel"""

    _scenarios = {}
    parameters = attrib(init=False, repr=False)
    steps = attrib(init=False, repr=False)
    dependencies = attrib(init=False, repr=False)
    dependency_index = attrib(init=False, repr=False)

    @classmethod
    def register_scenario(cls, name, **kwargs):
        """Register scenario"""
        if name in cls._scenarios:
            raise ModelScenarioAlreadyExist(f"The scenario [{name}] already exists.")
        args = getfullargspec(cls).kwonlyargs
        unknown = [k for k in kwargs if k not in args]
        if len(unknown) > 0:
            raise ModelScenarioParamDoesNotExist(
                f"The parameter(s) [{unknown}] do not exist."
            )
        cls._scenarios.update({name: kwargs})

    @classmethod
    def using_scenario(cls, name, **kwargs):
        """Using scenario"""
        defined_kwargs = cls._scenarios.get(name, None)
        if defined_kwargs is None:
            raise ModelScenarioDoesNotExist(f"The scenario [{name}] does not exist.")
        duplicate = [k for k in defined_kwargs if k in kwargs]
        if len(duplicate) > 0:
            msg = f"The following kwarg(s) are already defined in the scenario [{duplicate}]."
            raise ModelScenarioParamAlreadyExist(msg)
        return cls(**defined_kwargs, **kwargs)

    @property
    def scenarios(self):
        """List of scenarios"""
        return self._scenarios

    def visualize(self):
        """Visualize model"""
        return visualize_model(self)

    def audit(self, **kwargs):
        """Audit model"""
        return run_model_audit(model=self, **kwargs)

    def run(self):
        """Run model"""
        return run_model(self)


def create_attributes(footing):
    """Create attributes"""
    attributes = {}
    for arg_key, arg_val in footing.parameters.items():
        kwargs = {}
        if arg_val.default is not None:
            kwargs.update({"default": arg_val.default})
        kwargs.update({"kw_only": True, "validator": arg_val._create_validator()})
        attributes.update({arg_key: attrib(**kwargs)})

    args_attrib = attrib(init=False, repr=False, default=footing.parameters)
    steps_attrib = attrib(init=False, repr=False, default=footing.steps)
    dep_attrib = attrib(init=False, repr=False, default=footing.dependencies)
    dep_index = create_dependency_index(footing.dependencies)
    dep_index_attrib = attrib(init=False, repr=False, default=dep_index)
    return {
        **attributes,
        "parameters": args_attrib,
        "steps": steps_attrib,
        "dependencies": dep_attrib,
        "dependency_index": dep_index_attrib,
    }


def _create_parameters_section(parameters):
    def _clean_dtype(dtype):
        dtype_str = str(dtype)
        return dtype_str.replace("<class '", "").replace("'>", "")

    ret_str = "Parameters\n----------\n"
    for arg_k, arg_v in parameters.items():
        if arg_v.dtype is None:
            ret_str += f"{arg_k}\n\t{arg_v.description}\n"
        else:
            ret_str += f"{arg_k} : {_clean_dtype(arg_v.dtype)}\n\t{arg_v.description}\n"
    return ret_str


def _create_steps_section(steps):
    ret_str = "Steps\n-----\n"
    for idx, step in enumerate(steps):
        docstring = FunctionDoc(step["function"])
        ret_str += f"Step {idx} - {step['name']}\n"
        ret_str += "".join(["\t" + x + "\n" for x in docstring.get("Summary")])
    return ret_str


def _get_returns(function):
    parsed_doc = FunctionDoc(function)
    parameters = parsed_doc["Returns"]
    if parameters != []:
        ret_str = "\tReturns\n\t-------\n"
        for param in parameters:
            if param.type != "" and param.name == "":
                ret_str += f"\t{param.type}\n"
            if param.type != "" and param.name != "":
                ret_str += f"\t{param.name} : {param.type}\n"
            if param.desc != []:
                ret_str += "".join([f"\t\t{x}\n" for x in param.desc])
        return ret_str
    return ""


def _create_methods_section(function):
    ret_str = "Methods\n-------\n"
    ret_str += "run()\n\tExecutes the model.\n\n"
    ret_str += _get_returns(function)
    return ret_str


def create_model_docstring(description: str, parameters: dict, steps: list) -> str:
    """Create model docstring.

    Parameters
    ----------
    description : str
        A description of the model.
    parameters : dict
        A dict of the parameter assocated with the model.
    steps : list
        The steps in the model.

    Returns
    -------
    str
       The created docstring for the model.
    """

    docstring = f"{description}\n\n"
    docstring += _create_parameters_section(parameters) + "\n"
    docstring += _create_steps_section(steps) + "\n"
    docstring += _create_methods_section(steps[-1].get("function"))
    return docstring


def build_model(
    name: str, steps: List[Dict], description: str = None, scenarios: dict = None,
):
    """A factory function to build a model.

    A model is a sequential list of function calls. Defined parameters will become model inputs and
    any defined Dependents will link output from  one step as input to another.

    A model is a child of the BaseModel class with the type equal to the passed name parameter.

    Parameters
    ----------
    name : str
        The name to assign the model.
    steps : list[Dict]
        The list of steps the model will perform.
    description : str, optional
        A description of the model, by default None.
    scenarios : dict, optional
        Defined scenarios to pass to the  model, by default None.

    Returns
    -------
    name
        An object with type equal to the passed parameter name.

    See Also
    --------
    footings.model.BaseModel

    """
    footing = create_footing_from_list(name=name, steps=steps)
    attributes = create_attributes(footing)
    model = make_class(
        name, attrs=attributes, bases=(BaseModel,), slots=True, frozen=True, repr=False
    )
    model.__doc__ = create_model_docstring(description, footing.parameters, steps)
    if scenarios is not None:
        for k, v in scenarios.items():
            model.register_scenario(k, **v)
    return model
