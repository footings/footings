"""model.py"""

from typing import List, Dict, Optional, Any
from dask.delayed import single_key, rebuild, optimize
from dask.base import DaskMethodsMixin
from dask import threaded
from dask.context import globalmethod
from attr import attrs, attrib, make_class

from .table_schema import TableSchema
from .graph import _build_graph_get_parameters
from .ffunction import FFunction


@attrs(slots=True)
class BaseModel(DaskMethodsMixin):
    """BaseModel"""

    _table_schemas = attrib(init=False, repr=False, factory=dict)
    _parameters = attrib(init=False, repr=False, factory=dict)
    _graph = attrib(init=False, repr=False)
    _keys = attrib(init=False, repr=False)

    def __attrs_post_init__(self):
        items = {k: getattr(self, k) for k in self.__slots__ if k[0] != "_"}
        self._graph.update(**items)

    def __dask_graph__(self):
        return self._graph.graph

    def __dask_keys__(self):
        return self._keys

    def __dask_layers__(self):
        return (self._keys,)

    def __dask_tokenize__(self):
        return self._keys

    __dask_scheduler__ = staticmethod(threaded.get)
    __dask_optimize__ = globalmethod(optimize, key="delayed_optimize")

    def __dask_postcompute__(self):
        return single_key, ()

    def __dask_postpersist__(self):
        return rebuild, (self._keys, getattr(self, "_length", None))

    def __footings_meta__(self):
        return {}

    def __footings_table_schemas___(self):
        return self._table_schemas

    def __footings_parameters__(self):
        return self._parameters

    def __footings_steps_(self):
        return self._graph.steps

    def __footings_dag__(self):
        return self._graph.graph


def _footings_meta(meta):
    """ """
    return meta


def _create_attributes(table_schemas, parameters, graph, defaults, return_kws):
    tables = {
        k: attrib(default=getattr(defaults, k, None), validator=v._create_validator())
        for k, v in table_schemas.items()
    }
    params = {
        k: attrib(default=getattr(defaults, k, None), validator=v._create_validator())
        for k, v in parameters.items()
    }
    keys = [v for k, v in graph.keys.items() if k in return_kws]
    return {
        **tables,
        **params,
        "_table_schemas": attrib(init=False, repr=False, default=table_schemas),
        "_parameters": attrib(init=False, repr=False, default=parameters),
        "_graph": attrib(init=False, repr=False, default=graph),
        "_keys": attrib(init=False, repr=False, default=keys),
    }


# pylint: disable=bad-continuation
def build_model(
    model_name: str,
    table_schemas: List[TableSchema],
    instructions: List[FFunction],
    defaults: Optional[Dict[str, Any]] = None,
    # meta: Optional[Dict[str, Any]] = None,
    **kwargs,
):
    """ build_model """
    if isinstance(table_schemas, TableSchema):
        table_schemas = {table_schemas.name: table_schemas}
    elif isinstance(table_schemas, list):
        table_schemas = {t.name: t for t in table_schemas}

    if kwargs == {}:
        return_kws = table_schemas.keys()
    else:
        return_kws = kwargs

    graph, params = _build_graph_get_parameters(instructions, *table_schemas.keys())
    attributes = _create_attributes(table_schemas, params, graph, defaults, return_kws)
    model = make_class(
        name=model_name, attrs=attributes, bases=(BaseModel,), slots=True, frozen=True
    )

    return model
