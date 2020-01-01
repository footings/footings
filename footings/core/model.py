from typing import List, Dict, Tuple, Union, Optional

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
from .errors import DuplicateParameterError

# from .annotation import Setting, Column, CReturn, Frame, FReturn
# from .utils import _generate_message
# from .ffunction import FFunction


class Model(DaskMethodsMixin):
    """
    """

    def __init__(
        self,
        schemas: Dict[str, Schema],
        graph: Dict[str, Tuple],
        key: Union[str, List[str]],
        fuse: bool = False,
        model_meta: Optional[Dict] = None,
        **kwargs,
    ):
        self._model_meta = model_meta
        self._schemas = self._validate_schemas(schemas)
        self._graph = self._validate_graph(graph)
        self._parameters = self._get_parameters()
        self._key = self._valaidate_key(key, graph)
        self._dask_graph = create_dask_graph(
            self._graph, self._schemas, self._parameters, **kwargs
        )

    @property
    def model_meta(self):
        return self._model_meta

    @property
    def schemas(self):
        return self._schemas

    @property
    def graph(self):
        return self._graph

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
    def _validate_graph(graph):
        return graph

    @staticmethod
    def _valaidate_key(key, graph):
        return key

    def _get_parameters(self):
        d = {}
        for s in self.graph:
            if s.parameters is not None:
                for k, v in s.parameters.items():
                    if k in d:
                        if v != d[k]:
                            raise DuplicateParameterError(
                                f"{k} has duplicate parameters listed that do not match."
                            )
                    else:
                        d.update({k: v})
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
    def __call__(self, *args, **kwargs):
        self._dask_graph = update_dask_graph(**kwargs)
        return self


def create_dask_graph(steps, schemas=None, parameters=None, **kwargs):
    if schemas is not None and parameters is not None:
        d = {k: None for k in list(schemas.keys()) + list(parameters.keys())}
    elif schemas is not None and parameters is None:
        d = {k: None for k in list(schemas.keys())}
    elif schemas is None and parameters is not None:
        d = {k: None for k in list(parameters.keys())}
    else:
        raise ValueError("schemas and parameters cannot both be None")
    return {**d, **kwargs, **graph}


def update_dask_graph(**kwargs):
    pass
