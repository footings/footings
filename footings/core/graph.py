"""Objects tied to graph"""

from attr import attrs, attrib
from dask.delayed import delayed, Delayed, unpack_collections
import dask.dataframe as dd
from dask.highlevelgraph import HighLevelGraph
from dask import get
import pandas as pd

from .ffunction import FFunction
from .parameter import Parameter


class GraphDelayedPartitionError(Exception):
    """Cannot apply a delayed function when the using partitions"""


def _create_inputs(inputs, function, table_keys, parameters):
    def swap_args(arg, table_keys, parameters):
        if arg in table_keys.keys():
            return table_keys[arg]
        if arg in parameters.keys():
            return arg
        raise Exception()

    return (function, *tuple(swap_args(arg, table_keys, parameters) for arg in inputs))


def _create_output(outputs, function, table_keys):
    if len(outputs) > 1:
        out = []
        for output in outputs:
            out_tuple = (output, function.name)
            out.append(out_tuple)
            table_keys[output] = out_tuple
        return tuple(out), table_keys
    out_tuple = (outputs[0], function.name)
    table_keys[outputs[0]] = out_tuple
    return out_tuple, table_keys


def _build_graph_get_parameters(steps, *tables):
    table_keys = {table: table for table in tables}
    parameters = {}
    graph = {}
    for step in steps:
        if not isinstance(step, FFunction):
            raise TypeError(f"Each item in steps must be a FFunction")
        params = step.input_parameters
        if params != {}:
            parameters.update({v.name: v for v in params.values()})
        outputs, function, inputs = step.get_step_items()
        inputs = _create_inputs(inputs, function, table_keys, parameters)
        outputs, table_keys = _create_output(outputs, function, table_keys)
        graph.update({outputs: inputs})
    graph = {
        **{table: None for table in tables},
        **{parameter: None for parameter in parameters},
        **graph,
    }
    return Graph(graph, steps, table_keys), parameters


@attrs(slots=True)
class Graph:
    """Instructions"""

    graph = attrib()
    steps = attrib()
    keys = attrib()

    def update(self, **kwargs):
        """Update graph"""
        for k, v in kwargs.items():
            self.graph[k] = v


# class GraphCollection:
#     """
#     """
#
#     __slots__ = ("_key", "dask", "_length")
#
#     def __init__(self, key, dsk, length=None):
#         self._key = key
#         self.dask = dsk
#         self._length = length
#
#     def __dask_graph__(self):
#         return self.dask
#
#     def __dask_keys__(self):
#         return [self.key]
#
#     def __dask_layers__(self):
#         return (self.key,)
#
#     def __dask_tokenize__(self):
#         return self.key
#
#     @property
#     def key(self):
#         return self._key
#
class GraphCollection(Delayed):
    pass


# @attrs
# class _GraphBuilder:
#     graph = attrib(default=None)
#     key = attrib(factory=dict)
#     map_partition = attrib(default=False)
#
#     def add_step(self, function):
#         """Add step to graph"""
#         if not isinstance(function, FFunction):
#             raise TypeError(f"The function {function} needs to be of class {FFunction}")
#         from inspect import getfullargspec
#
#         delay = getattr(function, "delayed")
#         collapse = getattr(function, "collapse")
#         partition = getattr(function, "partition")
#         inputs = getattr(function, "inputs")
#         args = getfullargspec(function.function).args
#
#         if collapse is True:
#             self.map_partition = False
#         #if delay:
#         #    function = delayed(function)
#
#         if self.map_partition is False:
#             name = function.name
#             layer = (function, *[inputs[arg].name for arg in args])
#             graph = HighLevelGraph.from_collections(f"{name}-zz", layer, dependencies=())
#             self.graph = HighLevelGraph.merge(self.graph, graph)
#         else:
#             pass
#             # if delay:
#             #     msg = f"{function} cannot be set to delayed when applied to partiitons"
#             #     raise GraphDelayedPartitionError(msg)
#             # self.graph = dd.map_partitions(function, self.graph)
#         if partition is True:
#             self.map_partition = True
#
#     def add_table(self, name, table):
#         """Add table to graph"""
#         if table.dtype != pd.DataFrame:
#             raise NotImplementedError()
#         self._add_to_graph(table.name, table.to_pandas_dataframe())
#         self.key.update({name: table.name})
#
#     def add_parameter(self, name, parameter):
#         """Add parameter to graph"""
#         self._add_to_graph(parameter.name, parameter.default)
#         self.key.update({name: parameter.name})
#
#     def _add_to_graph(self, name, item):
#         if self.graph is None:
#             graph = HighLevelGraph.from_collections(f"{name}-zz", item, dependencies=())
#             self.graph = graph
#         else:
#             task, collections = unpack_collections(self.graph)
#             graph = HighLevelGraph.from_collections(f"{name}-zz", item, dependencies=collections)
#             self.graph = HighLevelGraph.merge(self.graph, graph)


@attrs
class _GraphBuilder:
    graph = attrib(default=None)
    key = attrib(factory=dict)
    map_partition = attrib(default=False)

    def add_step(self, function, collection):
        """Add step to graph"""
        if not isinstance(function, FFunction):
            raise TypeError(f"The function {function} needs to be of class {FFunction}")
        from inspect import getfullargspec

        collapse = getattr(function, "collapse")
        partition = getattr(function, "partition")
        inputs = getattr(function, "inputs")
        args = getfullargspec(function.function).args

        params = [v for k, v in inputs.items() if isinstance(v, Parameter)]
        # if len(params) > 0:
        #     delayed_params =

        if collapse:
            self.map_partition = False

        if self.map_partition:
            self.graph = dd.map_partitions(
                function, self.graph, meta={"a": int, "b": int, "add": int}
            )
        else:
            self.graph = function(self.graph)

        if partition:
            self.map_partition = True

    def add_table(self, name, table):
        """Add table to graph"""
        if table.dtype != pd.DataFrame:
            raise NotImplementedError()
        self._add_to_graph(table.name, table.to_pandas_dataframe())
        self.key.update({name: table.name})

    def add_parameter(self, name, parameter):
        """Add parameter to graph"""
        self._add_to_graph(parameter.name, parameter.default)
        self.key.update({name: parameter.name})

    def _add_to_graph(self, name, item):
        if self.graph is None:
            pass
        else:
            pass


def _create_graph_builder(steps, ddf):
    """ """
    graph = _GraphBuilder()

    # for name, table in tables.items():
    #     graph.add_table(name, table)
    # for step in steps:
    #     for arg, input_ in step.inputs.items():
    #         if isinstance(input_, Parameter):
    #             graph.add_parameter(arg, input_)

    graph.graph = ddf
    for step in steps:
        # print(step)
        graph.add_step(step, ddf)
    return graph
