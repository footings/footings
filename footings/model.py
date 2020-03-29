"""model.py"""

from inspect import getfullargspec
from functools import singledispatch

from attr import attrs, attrib, make_class

from .footing import Footing
from .audit import run_model_audit
from .visualize import visualize_model
from .utils import Dispatcher

__all__ = ["build_model"]

#########################################################################################
# established errors
#########################################################################################


class ModelScenarioAlreadyExist(Exception):
    """The scenario already exist."""


class ModelScenarioDoesNotExist(Exception):
    """The scenario does not exist."""


class ModelScenarioArgAlreadyExist(Exception):
    """The arugment passed to scenario already exist."""


class ModelScenarioArgDoesNotExist(Exception):
    """The argument passed does not exist."""


#########################################################################################
# model
#########################################################################################


# @singledispatch
# def output_src_as_set(output_src):
#     """Set output_src based on different types."""
#     msg = f"output_src_as_set has not been defined for type [{output_src}]."
#     raise NotImplementedError(msg)
#
#
# @output_src_as_set.register
# def _(output_src: tuple):
#     return set(output_src)
#
#
# @singledispatch
# def to_output_src(output_src, dict_):
#     """Set to_output_src based on different types."""
#     msg = f"to_output_src has not been defined for type [{output_src}]."
#     raise NotImplementedError(msg)
#
#
# @to_output_src.register
# def _(output_src: tuple, dict_):
#     if len(output_src) > 1:
#         return tuple(dict_[x] for x in output_src)
#     return dict_[output_src[0]]


OUTPUT_SRC_AS_SET = Dispatcher("output_src_as_set", parameters=("obj",))


@OUTPUT_SRC_AS_SET.register(obj=str(tuple))
def _(output_src):
    return set(output_src)


TO_OUTPUT_SRC = Dispatcher("to_output_src", parameters=("obj",))


@TO_OUTPUT_SRC.register(obj=str(tuple))
def _(output_src, dict_):
    if len(output_src) > 1:
        return tuple(dict_[x] for x in output_src)
    return dict_[output_src[0]]


def register_output(obj, as_set, to_output):
    """Tegister output"""
    OUTPUT_SRC_AS_SET.register(obj=str(type(obj)), function=as_set)
    TO_OUTPUT_SRC.register(obj=str(type(obj)), function=to_output)


def output_src_as_set(output_src):
    """Output src as set"""
    return OUTPUT_SRC_AS_SET(output_src=output_src, obj=str(type(output_src)))


def to_output_src(output_src, dict_):
    """To output src"""
    return TO_OUTPUT_SRC(output_src=output_src, dict_=dict_, obj=str(type(output_src)))


def create_dependency_index(dependencies, output_src):
    """Create dependency index"""
    store = output_src_as_set(output_src)
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
    ret.update({keys[-1]: output_src_as_set(output_src)})

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
        init_args = {v: getattr(model, k) for k, v in v.init_args.items()}
        dependent_args = {k: dict_[v] for k, v in v.dependent_args.items()}
        out = v.function(**init_args, **dependent_args, **v.defined_args)
        update_dict_(dict_, dependency_index[k], k, out)
    return to_output_src(model.output_src, dict_)


FOOTING_RESERVED_WORDS = [
    "scenarios",
    "args",
    "steps",
    "dependencies",
    "dependency_index",
    "output_src",
    "meta",
]


@attrs(slots=True, frozen=True)
class BaseModel:
    """BaseModel"""

    _scenarios = {}
    args = attrib(init=False, repr=False)
    steps = attrib(init=False, repr=False)
    dependencies = attrib(init=False, repr=False)
    dependency_index = attrib(init=False, repr=False)
    output_src = attrib(init=False, repr=False)
    meta = attrib(init=False, repr=False)

    @classmethod
    def register_scenario(cls, name, **kwargs):
        """Register scenario"""
        if name in cls._scenarios:
            raise ModelScenarioAlreadyExist(f"The scenario [{name}] already exists.")
        args = getfullargspec(cls).kwonlyargs
        unknown = [k for k in kwargs if k not in args]
        if len(unknown) > 0:
            raise ModelScenarioArgDoesNotExist(
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
            raise ModelScenarioArgAlreadyExist(msg)
        return cls(**defined_kwargs, **kwargs)

    @property
    def scenarios(self):
        """List of scenarios"""
        return self._scenarios

    def visualize(self):
        """Visualize model"""
        return visualize_model(self)

    def run(self, audit=False):
        """Run model"""
        if audit:
            return run_model_audit(self)
        return run_model(self)


def create_attributes(footing, output_src, meta):
    """Create attributes"""
    attributes = {}
    for arg_key, arg_val in footing.arguments.items():
        kwargs = {}
        if arg_val.default is not None:
            kwargs.update({"default": arg_val.default})
        kwargs.update({"kw_only": True, "validator": arg_val.create_validator()})
        attributes.update({arg_key: attrib(**kwargs)})

    arg_nms = tuple(footing.arguments.keys())
    args_attrib = attrib(init=False, repr=False, default=arg_nms)
    output_src_attrib = attrib(init=False, repr=False, default=output_src)
    steps_attrib = attrib(init=False, repr=False, default=footing.steps)
    dep_attrib = attrib(init=False, repr=False, default=footing.dependencies)
    dep_index = create_dependency_index(footing.dependencies, output_src)
    dep_index_attrib = attrib(init=False, repr=False, default=dep_index)
    meta_attrib = attrib(init=False, repr=False, default=meta)
    return {
        **attributes,
        "args": args_attrib,
        "output_src": output_src_attrib,
        "steps": steps_attrib,
        "dependencies": dep_attrib,
        "dependency_index": dep_index_attrib,
        "meta": meta_attrib,
    }


def create_docstring(attributes, description):
    """Create docstring"""
    # need to build description
    # need to build parameters
    # need to build returns
    # need to build doc_meta
    return attributes, description


def build_model(
    name, footing, description=None, output_src=None, scenarios=None, meta=None
):
    """Build model"""
    if not isinstance(footing, Footing):
        msg = (
            f"The object passed to footing must be of type {footing} not {type(footing)}."
        )
        raise TypeError(msg)
    if output_src is None:
        output_src = (list(footing.steps.keys())[-1],)
    attributes = create_attributes(footing, output_src, meta)
    model = make_class(
        name, attrs=attributes, bases=(BaseModel,), slots=True, frozen=True
    )
    model.__doc__ = create_docstring(attributes, description)
    if scenarios is not None:
        for k, v in scenarios.items():
            model.register_scenario(k, **v)
    return model
