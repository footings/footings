"""model.py"""

from inspect import getfullargspec
from warnings import warn

from attr import attrs, attrib, make_class
from dask.distributed import Client
from dask.local import get_sync
import dask.dataframe as dd
import pandas as pd

from .visualize import visualize_model


class ModelScenarioAlreadyExist(Exception):
    """The scenario already exist"""


class ModelScenarioDoesNotExist(Exception):
    """The scenario does not exist"""


class ModelParameterDoesNotExist(Exception):
    """The parameter does not exist"""


def run_model(model, audit, client):
    """Generic function to run a model"""
    if not issubclass(type(model), BaseModel):
        raise TypeError(f"Model must be a subtype of [{BaseModel}].")

    if audit is True:
        if client is not None:
            warn(f"Both audit and client are set to True. The client will be ignored.")
        raise NotImplementedError("Not implemented yet.")

    key = list(model.task_graph.keys())[-1]
    dask_graph = get_sync(model.task_graph, key)
    if client is not None:
        if not isinstance(client, Client):
            raise TypeError(f"client must be of type [{Client}].")
        return client.compute(dask_graph)
    return dask_graph.compute()


@attrs(slots=True)
class BaseModel:
    """BaseModel"""

    _scenarios = {}
    args = attrib(init=False, repr=False)
    task_graph = attrib(init=False, repr=False)

    def __attrs_post_init__(self):
        for arg in self.args:
            item = getattr(self, arg)
            if isinstance(item, pd.DataFrame):
                self.task_graph.update({arg: (dd.from_pandas, item, 1)})
            else:
                self.task_graph.update({arg: item})

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

    def run(self, audit=False, client=None):
        """Run model"""
        return run_model(self, audit, client)


def _create_attributes(plan):
    params = plan.get_parameters()
    tbl = {plan.tbl.name: attrib(kw_only=True, validator=plan.tbl.create_validator())}
    attributes = {}
    param_nms = []
    for param in params:
        kwargs = {}
        if param.default is not None:
            kwargs.update({"default": param.default})
        kwargs.update({"kw_only": True, "validator": param.create_validator()})
        attributes.update({param.name: attrib(**kwargs)})
        param_nms.append(param.name)

    arg_nms = [plan.tbl.name] + param_nms
    args = {"args": attrib(init=False, repr=False, default=arg_nms)}
    task_graph = {
        "task_graph": attrib(init=False, repr=False, default=plan.to_task_graph())
    }
    return {**tbl, **attributes, **args, **task_graph}


def _create_docstring(attributes, description, plan, doc_meta):
    # need to build description
    # need to build parameters
    # need to build returns
    # need to build doc_meta
    return description  # attributes, description, meta


def build_model(name, plan, description=None, meta=None, scenarios=None):
    """Build model"""
    attributes = _create_attributes(plan)
    model = make_class(
        name, attrs=attributes, bases=(BaseModel,), slots=True, frozen=True
    )
    model.__doc__ = _create_docstring(attributes, description, plan, meta)
    if scenarios is not None:
        for k, v in scenarios.items():
            model.register_scenario(k, **v)
    return model
