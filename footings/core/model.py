from typing import List, Dict, Tuple, Union, Optional, Any

import pandas as pd
from pyarrow import Schema
from dask.delayed import delayed, single_key, rebuild, optimize
from dask.base import DaskMethodsMixin
from dask import threaded
from dask.context import globalmethod

# import dask.dataframe as dd
# from dask.base import DaskMethodsMixin
# from toolz import curry

from .ffunction import FFunction
from .errors import ParameterDuplicateError, ParameterNotKnownError

# from .annotation import Setting, Column, CReturn, Frame, FReturn
from .utils import _generate_message

# from .ffunction import FFunction


class FootingsGraph:
    def __init__(self, graph):
        self.graph = graph
        self.steps = None

    def from_steps(
        self, steps,
    ):
        pass


class Model(DaskMethodsMixin):
    """
    """

    def __init__(
        self,
        schemas: Dict[str, Schema],
        instructions: Union[List[FFunction], Dict[str, Any]],
        key: Union[str, List[str]],
        fuse: bool = False,
        model_meta: Optional[Dict] = None,
        **kwargs,
    ):
        self._model_meta = model_meta
        self._schemas = self._validate_schemas(schemas)
        self._instructions = instructions
        if isinstance(instructions, dict):
            self._steps = self._get_steps(instructions)
        else:
            self._steps = self._validate_steps(instructions)
        self._parameters = self._get_parameters()
        self._key = self._valaidate_key(key)
        if isinstance(instructions, dict):
            self._dask = update_dask_graph(instructions, **kwargs)
        else:
            self._dask = create_dask_graph(
                self._schemas, self._steps, self._parameters, **kwargs
            )

    @property
    def model_meta(self):
        return self._model_meta

    @property
    def schemas(self):
        return self._schemas

    @property
    def steps(self):
        return self._steps

    @property
    def parameters(self):
        return self._parameters

    @property
    def key(self):
        if type(self._key) is List:
            return self._key
        else:
            return [self._key]

    @property
    def dask(self):
        return self._dask

    def __dask_graph__(self):
        return self._dask

    def __dask_keys__(self):
        return self.key

    def __dask_layers__(self):
        return (self.key,)

    def __dask_tokenize__(self):
        return self.key

    __dask_scheduler__ = staticmethod(threaded.get)
    __dask_optimize__ = globalmethod(optimize, key="delayed_optimize")

    def __dask_postcompute__(self):
        return single_key, ()

    def __dask_postpersist__(self):
        return rebuild, (self.key, getattr(self, "_length", None))

    @staticmethod
    def _validate_schemas(schemas):
        return schemas

    @staticmethod
    def _validate_steps(steps):
        return steps  # [s.generate_step() for s in steps]

    @staticmethod
    def _valaidate_key(key, graph):
        return key

    def _get_parameters(self):
        d = {}
        for s in self.steps:
            if s.parameters is not None:
                for s in s.parameters:
                    if s.name in d:
                        if s != d[k]:
                            raise DuplicateParameterError(
                                f"{k} has duplicate parameters listed that do not match."
                            )
                    else:
                        d.update({s.name: s})
        return d

    def register_run(
        self,
        run_name: str,
        has_scenarios: bool,
        transformer: Optional[callable] = None,
        return_schemas=None,
        **kwargs,
    ):
        pass

    def register_scenario(self, run_name: str, scenario_name: str, **kwargs):
        pass

    def run(self, run_name: str, scenario_name: Optional[str] = None):
        pass

    def transformer(self, function: callable, return_schemas):
        pass

    # need name mapper
    def __call__(self, **kwargs):
        self._dask = update_dask_graph(self._dask, **kwargs)
        # validate all keys are not None
        # ensure validation run against kwargs
        return self


def create_dask_graph(schemas, steps, parameters, **kwargs):
    def get_parameters(parameters, **kwargs):
        par_names = set([p.name for p in parameters])
        kwarg_names = set(kwargs.keys())
        diff = kwarg_names.difference(par_names)

        # if items in kwargs not in parameters raise error
        if len(kwargs) > 0:
            if len(diff) > 0:
                msg = "The following parameters are present in kwargs but are not known"
                raise ParameterNotKnownError(_generate_message(msg, diff))

        # validate assigned parameter values

        # parameters is kwargs + any value in parameters that is not in kwargs
        return {**kwargs, **{k: None for k in par_names.difference(kwarg_names)}}

    def steps_to_graph(steps):
        d = {}
        for i, s in enumerate(steps):
            if i == 0:
                in_ = s._inputs[0].src_name
            else:
                in_ = list(d.keys())[-1]
            out = s._outputs.src_name
            d.update({f"{s._outputs.src_name}-{s.name}": (s, in_)})
        return d

    parameters = get_parameters(parameters, **kwargs)
    step_graph = steps_to_graph(steps)

    return {**{k: None for k in schemas.keys()}, **parameters, **step_graph}


def update_dask_graph(graph, **kwargs):
    g = graph.copy()
    for k, v in kwargs.items():
        g[k] = v
    return g
