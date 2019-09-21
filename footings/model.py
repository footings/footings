

import pandas as pd
import dask.dataframe as dd
from dask.base import DaskMethodsMixin
from networkx import topological_sort

from .type import Setting
from .utils import _generate_message
from .function import _BaseFunction

class DaskComponents(DaskMethodsMixin):
    """
    """
    def compute(self, **kwargs):
        return super().compute(**kwargs)

    def persist(self, **kwargs):
        return super().persist(**kwargs)
    
    def visualize(self, filename='mydask', format=None, optimize_graph=False, **kwargs):
        return super().visualize(filename=filename, format=format, optimize_graph=optimize_graph, **kwargs)



def _build_model_graph(frame, registry, settings):
    G = registry._G
    # settings = settings if settings is not None else {}

    # get nodes that do not have a src as they will be either -
    # 1. from the starting frame or
    # 2. a setting
    nodes_check_frame = []
    nodes_check_settings = []
    for n, d in G.nodes(data=True):
        if 'src' not in d and d['class'] != Setting:
            nodes_check_frame.append(n)
        elif 'src' not in d and d['class'] == Setting:
            nodes_check_settings.append(n)
    
    print(nodes_check_frame)
    for n in nodes_check_frame:
        assert n in frame
        G.nodes[n]['src'] = frame
    
    if len(nodes_check_settings) > 0 and settings == {}:
        msg = """The following items are identified as Settings in the Registry, 
            but no Settings provided - 
            """
        raise AssertionError(_generate_message(msg, nodes_check_settings))

    missing_settings = []
    for n in nodes_check_settings:
        if n in settings:
            G[n]['value'] = settings[n]
        else:
            if G[n]['default'] is not None:
                G[n]['value'] = G[n]['default']
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
    func_list = [G.nodes[n]['src'] for n in sorted_nodes if callable(G.nodes[n]['src'])]
    return func_list


class FootingsModel(DaskComponents):
    """
    """
    def __init__(self, frame=None, registry=None, settings=None, 
                 calculate=None, scenario=None):
        
        assert type(frame) is dd.DataFrame
        if calculate is not None:
            assert calculate in [n for n, d in registry._G.nodes(data=True) if 'src' in d]

        #self.registry = registry
        #self.settings = settings
        #self.calculate = calculate
        #self.scenario = scenario
        
        G = _build_model_graph(frame, registry, settings)
        func_list = _get_functions(G, calculate)
        df = frame.copy()
        for f in func_list:
            df = f(df)

        self.frame = frame
        self._G = G


class MultiFModel(DaskComponents):
    pass




def model(frame, calculate, calc_set, asn_set, settings=None, scenario=None, **kwargs):
    if isinstance(frame, dd.DataFrame):
        pass
    elif isinstance(frame, pd.DataFrame):
        frame = dd.from_pandas(frame, **kwargs)
    # consider adding special footings frame
    else:
        raise TypeError('frame must be a pandas or dask DataFrame')
    
    # need to validate mapping

    #return Model(frame, calculate, calc_set, asn_set, settings, scenario)
    pass



class ModelMethods:
    def compute(self, *args, **kwargs):
        return self._frame.compute(*args, **kwargs)

    def persist(self, *args, **kwargs):
        return self._frame.persist(*args, **kwargs)

    def visualize_dask(self, *args, **kwargs):
        return self._frame.visualize(*args, **kwargs)

    def visualize_frame(self, *args, **kwargs):
        return self

    def audit(self, *args, **kwargs):
        return self


