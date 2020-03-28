"""model.py"""

from inspect import getfullargspec
from functools import singledispatch

from attr import attrs, attrib, make_class

from .footing import Footing
from .audit import run_model_audit
from .visualize import visualize_model

__all__ = ["build_model"]

#########################################################################################
# established errors
#########################################################################################


class ModelScenarioAlreadyExist(Exception):
    """The scenario already exist"""


class ModelScenarioDoesNotExist(Exception):
    """The scenario does not exist"""


class ModelParameterDoesNotExist(Exception):
    """The parameter does not exist"""


#########################################################################################
# model
#########################################################################################


@singledispatch
def set_capture(x):
    """Set capture based on different types."""
    raise NotImplementedError(f"set_capture has not been defined for type [{x}].")


@set_capture.register(tuple)
def _(x):
    return set(x)


def create_dependency_index(dependencies, capture):
    """Create dependency index"""
    store = set_capture(capture)
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
    ret.update({keys[-1]: set(capture)})

    ret = {
        v[0]: v[1].intersection(set(keys[: (idx + 1)]))
        for idx, v in enumerate(ret.items())
    }
    return ret


def update_store(store, dependency_index, step, output):
    """Update store"""
    store.update({step: output})
    keys = list(store.keys())
    for key in keys:
        if key not in dependency_index:
            del store[key]


@singledispatch
def output_store(x, dict_):
    """Set capture based on different types."""
    raise NotImplementedError(f"output_store has not been defined for type [{x}].")


@output_store.register(tuple)
def _(x, dict_):
    if len(x) > 1:
        return tuple(dict_[x] for x in x)
    return dict_[x[0]]


def run_model(model):
    """Generic function to run a model"""
    if not issubclass(type(model), BaseModel):
        raise TypeError(f"The model passed must be a subclass of {BaseModel}.")

    store = {}
    steps = model.steps
    dependency_index = model.dependency_index

    for k, v in steps.items():
        if k not in dependency_index[k]:
            continue
        init_args = {v: getattr(model, k) for k, v in v.init_args.items()}
        dependent_args = {k: store[v] for k, v in v.dependent_args.items()}
        out = v.function(**init_args, **dependent_args, **v.defined_args)
        update_store(store, dependency_index[k], k, out)
    return output_store(model.capture, store)


FOOTING_RESERVED_WORDS = [
    "scenarios",
    "args",
    "steps",
    "dependencies",
    "dependency_index",
    "capture",
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
    capture = attrib(init=False, repr=False)
    meta = attrib(init=False, repr=False)

    @classmethod
    def register_scenario(cls, name, **kwargs):
        """Register scenario"""
        if name in cls._scenarios:
            raise ModelScenarioAlreadyExist(f"The scenario [{name}] already exist.")
        args = set(getfullargspec(cls))
        unknown = set(kwargs.keys()) - args
        if len(unknown) == 1:
            raise ModelParameterDoesNotExist(f"The parameter [{unknown}] does not exist.")
        if len(unknown) > 1:
            raise ModelParameterDoesNotExist(f"The parameters [{unknown}] do not exist.")
        cls._scenarios.update({name: kwargs})

    @classmethod
    def using_scenario(cls, name, **kwargs):
        """Using scenario"""
        if name not in cls._scenarios:
            raise ModelScenarioDoesNotExist(f"The scenario [{name}] does not exist.")
        return cls(**kwargs, **cls._scenarios.get(name))

    def visualize(self):
        """Visualize model"""
        return visualize_model(self)

    def run(self, audit=False):
        """Run model"""
        if audit:
            return run_model_audit(self)
        return run_model(self)


def create_attributes(footing, capture, meta):
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
    capture_attrib = attrib(init=False, repr=False, default=capture)
    steps_attrib = attrib(init=False, repr=False, default=footing.steps)
    dep_attrib = attrib(init=False, repr=False, default=footing.dependencies)
    dep_index = create_dependency_index(footing.dependencies, capture)
    dep_index_attrib = attrib(init=False, repr=False, default=dep_index)
    meta_attrib = attrib(init=False, repr=False, default=meta)
    return {
        **attributes,
        "args": args_attrib,
        "capture": capture_attrib,
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


def build_model(name, footing, description=None, capture=None, scenarios=None, meta=None):
    """Build model"""
    if not isinstance(footing, Footing):
        msg = (
            f"The object passed to footing must be of type {footing} not {type(footing)}."
        )
        raise TypeError(msg)
    if capture is None:
        capture = (list(footing.steps.keys())[-1],)
    attributes = create_attributes(footing, capture, meta)
    model = make_class(
        name, attrs=attributes, bases=(BaseModel,), slots=True, frozen=True
    )
    model.__doc__ = create_docstring(attributes, description)
    if scenarios is not None:
        for k, v in scenarios.items():
            model.register_scenario(k, **v)
    return model
