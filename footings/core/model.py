"""model.py"""

from typing import List, Union, Optional
from dataclasses import make_dataclass, field
from warnings import warn
from dask.delayed import rebuild, optimize
from dask.base import DaskMethodsMixin
from dask import threaded
from dask.context import globalmethod
import pandas as pd

from .table_schema import TableSchema
from .ffunction import FFunction
from .parameter import Parameter

from collections import namedtuple

Results = namedtuple("Results", "output parameters")


def single_key(seq, *args):
    """ Pick out the only element of this list, a list of keys """
    print(seq)
    return Results(output=seq[0], parameters="zzz")  # (seq[0], args)


_DASK_NAMESPACE = {
    "keys": property(lambda self: self._keys),
    "dask": property(lambda self: self._dask),
    "__dask_graph__": lambda self: self._dask,
    "__dask_keys__": lambda self: self._keys,
    "__dask_layers__": lambda self: (self._keys,),
    "__dask_tokenize__": lambda self: self._keys,
    "__dask_scheduler__": staticmethod(threaded.get),
    "__dask_optimize__": globalmethod(optimize, key="delayed_optimize"),
    "__dask_postcompute__": lambda self: (single_key, (self.dask, self.keys)),
    "__dask_postpersist__": lambda self: (
        rebuild,
        (self.keys, getattr(self, "_length", None)),
    ),
}


def _validate_instructions(table_schemas, instructions):
    """ """
    return instructions


def _validate_keys(keys):
    """ """
    pass


def _instructions_to_dict(table_schemas, instructions):
    """ """
    pass


def _get_parameters(instructions):
    """ """
    p = {}
    for k, v in instructions.items():
        if isinstance(v, Parameter):
            if k != v.name:
                msg = "The key [{k}] and name [{v.name}] do not match. \
                    {k} will be used as the default."
                warn(msg)
            p.update({k: v})
    return p


def _get_graph_fields_keys(instructions, tables, parameters, keys, **kwargs):
    """ """
    fields = []
    graph = instructions.copy()

    for k, v in tables.items():
        if k in kwargs:
            fields.append((k, pd.DataFrame, field(default=kwargs[k])))
        else:
            fields.append((k, pd.DataFrame))
        graph.update({k: None})

    for k, v in parameters.items():
        if k in kwargs:
            fields.append(
                (k, v.dtype, field(default=kwargs[k], metadata=v.generate_meta()))
            )
        else:
            fields.append((k, v.dtype, field(metadata=v.generate_meta())))
        graph.update({k: None})

    if keys is None:
        keys = [list(graph.keys())[-1]]
    else:
        _validate_keys(keys)

    return graph, fields, keys


def _create_setter_and_post_init_funcs(table_schemas, parameters):
    """ """
    table_nms = list(table_schemas.keys())
    param_nms = list(parameters.keys())

    def setter(self, name, value):
        if name in table_nms:
            if "_dask" in self.__dict__:
                table_schemas[name].valid(value)
                self._dask[name] = value
        elif name in param_nms:
            if "_dask" in self.__dict__:
                parameters[name].valid(value)
                self._dask[name] = value
        self.__dict__[name] = value

    def post_init(self):
        # create copy on init to prevent all models from sharing the same dict reference
        self._dask = self._dask.copy()
        self._keys = self._keys.copy()

        for name in table_nms + param_nms:
            if name in table_nms:
                value = getattr(self, name)
                table_schemas[name].valid(value)
                self._dask[name] = value
            elif name in param_nms:
                value = getattr(self, name)
                parameters[name].valid(value)
                self._dask[name] = value

    return setter, post_init


def _footings_meta(meta):
    """ """
    return {}


def _get_functions(graph):
    """ """
    return {
        v[0].__name__: v[0]
        for k, v in graph.items()
        if isinstance(v, tuple) and callable(v[0])
    }


def build_model(
    model_name: str,
    table_schemas,
    instructions,
    keys: Optional[Union[str, List[str]]],
    meta: Optional[dict] = None,
    **kwargs,
):
    """ build_model """
    if isinstance(table_schemas, TableSchema):
        table_schemas = {table_schemas.name: table_schemas}
    elif isinstance(table_schemas, list):
        table_schemas = {t.name: t for t in table_schemas}

    if isinstance(instructions, dict):
        _validate_instructions(table_schemas, instructions)
    elif isinstance(instructions, list):
        instructions = _instructions_to_dict(table_schemas, instructions)
    else:
        raise TypeError("instructions must be a dict or list")

    parameters = _get_parameters(instructions)
    graph, fields, keys = _get_graph_fields_keys(
        instructions, table_schemas, parameters, keys, **kwargs
    )
    setter, post_init = _create_setter_and_post_init_funcs(table_schemas, parameters)
    namespace = {
        "_dask": graph,
        "_keys": keys,
        "__setattr__": setter,
        "__post_init__": post_init,
        "__footings_meta__": _footings_meta(meta),
        "__footings_table_schemas__": table_schemas,
        "__footings_parameters__": parameters,
        "__footings_functions__": _get_functions(graph),
        "__footings_dag__": graph.copy(),  # copy to prevent updates (_dask gets updated)
        **_DASK_NAMESPACE,
    }

    model = make_dataclass(
        cls_name=model_name, fields=fields, bases=(DaskMethodsMixin,), namespace=namespace
    )

    return model
