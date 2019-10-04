import pandas as pd
import dask.dataframe as dd
from dask.base import DaskMethodsMixin
from networkx import topological_sort

from .annotation import Setting
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


def _build_model_graph(frame, registry, settings):
    G = registry._G

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

    for n in nodes_check_frame:
        assert n in frame.columns
        G.nodes[n]["src"] = frame

    if len(nodes_check_settings) > 0 and settings == {}:
        msg = """The following items are identified as Settings in the Registry, 
            but no Settings provided - 
            """
        raise AssertionError(_generate_message(msg, nodes_check_settings))

    missing_settings = []
    for n in nodes_check_settings:
        if n in settings:
            G[n]["value"] = settings[n]
        else:
            if G[n]["default"] is not None:
                G[n]["value"] = G[n]["default"]
            else:
                missing_settings.append(n)

    if len(missing_settings) > 0:
        msg = """The following items are identified as Settings in the Registry, 
            but are not present in the model Settings parameter or do not have a 
            default value in the registry - 
            """
        raise AssertionError(_generate_message(msg, missing_settings))

    return G


def _get_functions(G, calculate):
    sorted_nodes = topological_sort(G)
    func_list = [G.nodes[n]["src"] for n in sorted_nodes if callable(G.nodes[n]["src"])]
    return func_list


class Model(DaskComponents):
    """
    """

    def __init__(
        self, frame=None, registry=None, settings=None, calculate=None, **kwargs
    ):

        assert type(frame) is dd.DataFrame
        if calculate is not None:
            assert calculate in [n for n, d in registry._G.nodes(data=True) if "src" in d]

        # self.registry = registry
        # self.settings = settings
        # self.calculate = calculate
        # self.scenario = scenario

        G = _build_model_graph(frame, registry, settings)
        func_list = _get_functions(G, calculate)
        df = frame.copy()
        for f in func_list:
            df = f(df)

        self._frame = df
        self._G = G
