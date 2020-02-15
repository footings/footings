"""model.py"""

# from typing import List
from inspect import getfullargspec

from dask.base import dont_optimize
from dask.context import globalmethod
from dask import threaded
from dask.delayed import optimize, single_key, rebuild, delayed
import dask.dataframe as dd
from attr import attrs, attrib, make_class

from .parameter import Parameter
from .schema import TblSchema
from .utils import PriorStep

USE_PARTITION_WITH_CLIENT_ONLY = True

class ModelScenarioAlreadyExist(Exception):
    """The scenario already exist"""


class ModelScenarioDoesNotExist(Exception):
    """The scenario does not exist"""


class ModelParameterDoesNotExist(Exception):
    """The parameter does not exist"""


@attrs(slots=True)
class _BaseModel:
    """BaseModel"""
    _dask = attrib(init=False)
    _key = attrib(init=False)
    _scenarios = {}
    _plan = []
    
    def __attrs_post_init__(self):
        self._dask = _create_dask_graph

    def __dask_graph__(self):
        return self._dask

    def __dask_keys__(self):
        return [self._key]

    def __dask_layers__(self):
        return (self._key,)

    def __dask_tokenize__(self):
        return self._key

    __dask_optimize__ = globalmethod(
        optimize, key="dataframe_optimize", falsey=dont_optimize
    )
    __dask_scheduler__ = staticmethod(threaded.get)

    def __dask_postcompute__(self):
        return single_key, ()

    def __dask_postpersist__(self):
        return rebuild, (self._key, getattr(self, "_length", None))

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



def _create_attributes(plan):
    args = plan.get_input_args()
    attributes = {}
    for arg in args:
        attribute = {}
        if isinstance(arg, Parameter):
            if arg.default is not None:
                attribute.update({"default": arg.default})
        attribute.update({"kw_only": True, "validator": arg.create_validator()})
        attributes.update({arg.name: attrib(**attribute)})
    return attributes


def _create_docstring(attributes, description, plan, doc_meta):
    # need to build description
    # need to build parameters
    # need to build returns
    # need to build doc_meta
    return description  # attributes, description, meta


def build_model(name, plan, description=None, doc_meta=None, scenarios=None):
    """Build model"""
    # pylint: disable=protected-access
    attributes = _create_attributes(plan)
    model = make_class(
        name, attrs=attributes, bases=(_BaseModel,), slots=True, frozen=True
    )
    func_default, func_client, func_audit = _create_functions(plan)
    model._plan = plan
    model.__doc__ = _create_docstring(attributes, description, plan, doc_meta)
    if scenarios is not None:
        for k, v in scenarios.items():
            model.register_scenario(k, **v)
    return model
