from networkx import DiGraph, ancestors, descendants
from .function import _BaseFunction
from .annotation import Setting


class Registry:
    """
    
    """

    # need to test tuple length 2
    # need to ensure not a duplicate
    def __init__(self, *functions, name=None):
        self._G = DiGraph()
        self.name = name
        for f in functions:
            self.register(f)

    def register(self, *functions):
        def inner(f):
            assert issubclass(type(f), _BaseFunction)
            self._G.add_nodes_from(f.columns_input)
            self._G.add_nodes_from(f.setting_input)
            self._G.add_nodes_from(f.columns_output)
            inp = [x[0] for x in f.columns_input + f.setting_input]
            out = [x[0] for x in f.columns_output]
            self._G.add_edges_from([(i, o) for i in inp for o in out])
            return self

        for f in functions:
            inner(f)
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

    def settings(self):
        return [(k, v) for k, v in self._G.nodes(data=True) if v["class"] == Setting]

    def __len__(self):
        return len(self._G)

    # def __repr__(self):
    #     pass
