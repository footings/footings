import pandas as pd
import dask.dataframe as dd
from pyarrow import Schema
from dask.base import DaskMethodsMixin
from networkx import topological_sort
from functools import partial

from .annotation import Setting, Column, CReturn, Frame, FReturn
from .utils import _generate_message
from .function import _BaseFunction


class DaskComponents:
    """
    """

    def compute(self, **kwargs):
        return self._frame.compute(**kwargs)

    def persist(self, **kwargs):
        return self._frame.persist(**kwargs)

    def visualize_graph(
        self, filename="mydask", format=None, optimize_graph=False, **kwargs
    ):
        return self.frame.visualize(
            filename=filename, format=format, optimize_graph=optimize_graph, **kwargs
        )

    def visualize_frame(
        self, filename="mydask", format=None, optimize_graph=False, **kwargs
    ):
        return self.frame.visualize(
            filename=filename, format=format, optimize_graph=optimize_graph, **kwargs
        )


class FootingsModelMethods:
    """
    """

    def audit(self, *args, **kwargs):
        pass

    def reduce_by(self, function, *args, **kwargs):
        pass

    def summary(self):
        pass

    def details(self):
        pass


def _build_model_graph(schema, registry, settings):
    G = registry._G.copy()

    # get nodes that do not have a src as they will be either -
    # 1. from the starting frame or
    # 2. a setting
    nodes_check_frame = []
    nodes_check_settings = []
    for n, d in G.nodes(data=True):
        if "src" not in d and d["class"] != Setting:
            nodes_check_frame.append(n)
        elif "src" not in d and d["class"] == Setting:
            nodes_check_settings.append(n)

    missing_nodes = []
    for n in nodes_check_frame:
        if n not in schema.names:
            missing_nodes.append(n)
    if len(missing_nodes) > 0:
        msg = """The following columns are expected in the input frame but are 
        missing - 
        """
        raise AssertionError(_generate_message(msg, missing_nodes))
    G.nodes[n]["src"] = "frame"

    if len(nodes_check_settings) > 0 and settings == {}:
        msg = """The following items are identified as Settings in the Registry, 
            but no Settings provided - 
            """
        raise AssertionError(_generate_message(msg, nodes_check_settings))

    missing_settings = []
    for n in nodes_check_settings:
        if n not in settings and G.nodes[n]["default"] is None:
            missing_settings.append(n)

    if len(missing_settings) > 0:
        msg = """The following items are identified as Settings in the Registry,
            but are not present in the model Settings parameter or do not have a
            default value in the registry -
            """
        raise AssertionError(_generate_message(msg, missing_settings))

    return G


def _to_df_function(function, settings, input_columns, output_columns):
    assert len(output_columns) == 1, "output_columns can only be length 1"
    ret = list(output_columns.keys())[0]

    def wrapper(_df):
        exp = lambda x: function(**{k: x[k] for k in input_columns.keys()}, **settings)
        _df = _df.assign(**{ret: exp})
        return _df

    wrapper.__doc__ = function.__doc__
    return wrapper


def _to_ff_function(function, settings, input_columns, output_columns):
    if type(function.__annotations__["return"]) == CReturn:
        return _to_df_function(function, settings, input_columns, output_columns)
    else:
        return partial(function, **settings)


def _set_settings(k, v, settings):
    if settings is None:
        if v["default"] is not None:
            return v["default"]
        else:
            raise AssertionError("Settings is empty and no default is provided")
    elif k in settings:
        if settings[k] is not None:
            return settings[k]
        else:
            raise AssertionError(k + " is in settings but is None")
    else:
        raise AssertionError(k + " not in settings and default value is not provided")


def _get_instructions(G, settings=None, calculate=None):

    sorted_nodes = topological_sort(G)
    func_nodes = [
        n for n in sorted_nodes if "src" in G.nodes[n] and callable(G.nodes[n]["src"])
    ]

    i = 1
    d = {}
    func_list = []
    for n in func_nodes:
        src = G.nodes[n]["src"]
        if src.__name__ not in func_list:
            # get func inputs and split into columns and settings
            anno = G.nodes[n]["src"].__annotations__
            in_set = {
                k: _set_settings(k, v, settings)
                for k, v in anno.items()
                if type(v) == Setting
            }
            if type(anno["return"]) == CReturn:
                in_cols = {k: v.dtype for k, v in anno.items() if type(v) == Column}
                out_cols = anno["return"].columns
            else:
                in_cols = {k: v.columns for k, v in anno.items() if type(v) == Frame}
                out_cols = anno["return"].columns

            # verify inputs exist
            params = list(in_cols.keys()) + list(in_set.keys())
            pred = {x: G.nodes[x] for x in G.predecessors(n)}
            assert all([p in params for p in pred])

            d[i] = {
                "ftype": G.nodes[n]["ftype"],
                "src_name": G.nodes[n]["src"].__name__,
                "src": G.nodes[n]["src"],
                "settings": in_set,
                "input_columns": in_cols,
                "output_columns": out_cols,
            }
            i += 1
            func_list.append(src.__name__)

    return d


def _combine_functions(instructions):
    def func_model(_df):
        # need to incorporate to_ff_function
        for k, v in instructions.items():
            _df = _to_ff_function(
                v["src"], v["settings"], v["input_columns"], v["output_columns"]
            )(_df)
        return _df

    return func_model


def _build_meta(frame, instructions):
    f = {i: str(v) for i, v in frame.dtypes.iteritems()}
    i = {c: d for k, v in instructions.items() for c, d in v["output_columns"].items()}
    return {**f, **i}


class ModelTemplate:
    """
    
    """

    def __init__(
        self,
        frame=None,
        registry=None,
        settings=None,
        calculate=None,
        scenario=None,
        stochastic=None,
        simulate=None,
        step=None,
    ):

        if calculate is not None:
            pass

        if scenario is not None:
            pass

        if stochastic is not None:
            pass

        if simulate is not None:
            pass

        if step is not None:
            pass

        G = _build_model_graph(Schema.from_pandas(frame), registry, settings)
        instr = _get_instructions(G, settings, calculate)
        func_combined = _combine_functions(instr)
        meta = _build_meta(frame, instr)

        self.functions = func_combined
        self.directions = instr
        self.description = None
        self.meta = meta


class ModelFromTemplate(DaskComponents):
    """
    
    """

    def __init__(self, frame, template, **kwargs):
        # run model
        ddf = frame.copy()

        # consider how step is done
        ddf = ddf.map_partitions(template.functions, meta=template.meta)

        self.description = template.description
        self.directions = template.directions
        self._frame = ddf

    def sub(self, x, y):
        pass

    def audit(self):
        pass

    def reduce_func(self):
        pass


class Model(ModelFromTemplate):
    """

    """

    def __init__(
        self,
        frame=None,
        registry=None,
        settings=None,
        calculate=None,
        scenario=None,
        stochastic=None,
        simulate=None,
        step=None,
        **kwargs,
    ):
        template = ModelTemplate(
            frame._meta, registry, settings, calculate, scenario, stochastic, step
        )
        self.template = template
        super().__init__(frame, template, **kwargs)


def as_model_template(**kwargs):
    template = ModelTemplate(kwargs)

    def wrapper(*args, **kwargs):
        return Model(args, kwargs, template=template)

    return wrapper


#     if not function:
#         return partial(AssumptionDeterministic, **kwargs)
#
#     @wraps(function)
#     def wrapper(function):
#         return AssumptionDeterministic(function, **kwargs)
#
#     return wrapper(function)
