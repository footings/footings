from networkx import (
    DiGraph,
    ancestors,
    descendants,
    topological_sort,
    get_node_attributes,
)
from .function import _BaseFunction
from .annotation import Setting, Column
from .utils import is_meta_like
from .calculation import Calculation
from .assumption import AssumptionDeterministic, AssumptionStochastic


class Registry:
    """
    
    """

    def __init__(self, *functions, **frames):
        self._functions = None  # updated when register is called
        self._starting_frame_meta = None  # updated when register is called
        self._G = DiGraph()
        self.register(*functions, **frames)

    def register(self, *functions, **frames):
        assert (
            len(frames) < 2
        ), "frames can only be passed a single key word of starting_frame_meta"
        if "starting_frame_meta" in frames:
            assert is_meta_like(frames.get("starting_frame_meta"))

            def inner_meta(df):
                d = df.copy().reset_index().dtypes.to_dict()
                for c, d in d.items():
                    self._G.add_node(
                        c,
                        value="Column",
                        dtype=d,
                        src=df,
                        src_name="starting_frame_meta",
                        src_type=type(df),
                        primary=True,
                    )
                return self

            inner_meta(frames.get("starting_frame_meta"))
            self._add_starting_frame_meta(frames.get("starting_frame_meta"))

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
            self._add_function(f)

        return self

    @property
    def functions(self):
        return self._functions

    def _add_function(self, *function):
        if self._functions is None:
            self._functions = function
        else:
            self._functions += function

    @property
    def starting_frame_meta(self):
        return self._starting_frame_meta

    def _add_starting_frame_meta(self, starting_frame_meta):
        assert self._starting_frame_meta is None, "a value already set for starting frame"
        self._starting_frame_meta = starting_frame_meta.dtypes.to_dict()

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

    def get_ordered_functions(self):
        sorted_nodes = topological_sort(self._G)
        func_list = []
        for n in sorted_nodes:
            n_data = self._G.nodes(data=True)[n]
            if issubclass(n_data["src_type"], _BaseFunction):
                if n_data["src"] not in func_list:
                    func_list.append(n_data["src"])
        return func_list

    def valid(self):
        pass

    def __len__(self):
        return len(self._G)

    # def __repr__(self):
    #     pass
