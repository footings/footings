from networkx import DiGraph, ancestors, descendants, topological_sort
from .function import _BaseFunction
from .annotation import Setting, Column
from .utils import is_meta_like
from .calculation import Calculation
from .assumption import AssumptionDeterministic, AssumptionStochastic


class Registry:
    """
    
    """

    def __init__(self, *functions, **frame_metas):
        assert len(frame_metas) <= 1, "cannot pass in more than 1 frame_meta"
        self._G = DiGraph()
        self.register(*functions, **frame_metas)

    def register(self, *functions, **frame_metas):
        def inner_meta(k, v):
            assert is_meta_like(v)
            d = v.copy().reset_index().dtypes.to_dict()
            for c, d in d.items():
                self._G.add_node(
                    c,
                    value="Column",
                    dtype=d,
                    src=v,
                    src_name=k,
                    src_type=type(v),
                    primary=True,
                )
            return self

        for k, v in frame_metas.items():
            inner_meta(k, v)

        def inner_func(f):
            assert issubclass(type(f), _BaseFunction)
            for k, v in f.input_columns.items():
                if self._G.has_node(k) == False:
                    self._G.add_node(k, value="Column", dtype=v)
            for k, v in f.settings.items():
                self._G.add_node(
                    k,
                    value="Setting",
                    setting=v,
                    src=f,
                    src_name=f.name,
                    src_type=type(f),
                )
            for k, v in f.output_columns.items():
                self._G.add_node(
                    k, value="Column", dtype=v, src=f, src_name=f.name, src_type=type(f)
                )
            inp = [x for x in list(f.input_columns.keys()) + list(f.settings.keys())]
            out = [x for x in list(f.output_columns.keys())]
            self._G.add_edges_from([(i, o) for i in inp for o in out])
            return self

        for f in functions:
            inner_func(f)

        return self

    def remove(self, *items):
        for i in items:
            self._G.remove_node(i)

    def list_registered(self):
        return [k for k, v in self._G.nodes(data=True)]

    def predecesors(self, column: str):
        return list(self._G.predecessors(column))

    def ancestors(self, column: str):
        return list(ancestors(self._G, column))

    def successors(self, column: str):
        return list(self._G.successors(column))

    def descendants(self, column: str):
        return list(descendants(self._G, column))

    def get_settings(self):
        return {k: v for k, v in self._G.nodes(data=True) if v["value"] == "Setting"}

    def get_functions(self):
        return {
            k: v
            for k, v in self._G.nodes(data=True)
            if issubclass(v["src_type"], _BaseFunction)
        }

    def get_calculations(self):
        return {k: v for k, v in self._G.nodes(data=True) if v["src_type"] == Calculation}

    def get_deterministic_assumptions(self):
        return {
            k: v
            for k, v in self._G.nodes(data=True)
            if v["src_type"] == AssumptionDeterministic
        }

    def get_stochastic_assumptions(self):
        return {
            k: v
            for k, v in self._G.nodes(data=True)
            if v["src_type"] == AssumptionStochastic
        }

    def get_primary_frame(self):
        df = None
        for k, v in self._G.nodes(data=True):
            if "primary" in v and df is None:
                if v["primary"]:
                    df = v["src"]
                    break

        return df

    def get_columns_from_frames(self):
        return {
            k: v
            for k, v in self._G.nodes(data=True)
            if issubclass(v["src_type"], _BaseFunction) == False
        }

    def valid(self):
        pass

    def __len__(self):
        return len(self._G)

    # def __repr__(self):
    #     pass
