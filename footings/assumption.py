from functools import wraps, partial

from .function import _BaseFunction


class Assumption:
    """
    A class to identify assumptions specifically built for the footings framework.
    """

    def __init__(self, function, edge_kws: dict = None, node_kws: dict = None):
        if node_kws is None:
            node_kws = {"src": "assumption"}
        else:
            assert "src" not in node_kws, "src not allowed as node_kws"
            node_kws.update({"src": "assumption"})
        super().__init__(function, edge_kws, node_kws)


# inspiration from
# https://stackoverflow.com/questions/10294014/python-decorator-best-practice-using-a-class-vs-a-function
def as_assumption(function=None, **kwargs):
    if not function:
        return partial(Assumption, **kwargs)

    @wraps(function)
    def wrapper(function):
        return Assumption(function, **kwargs)

    return wrapper(function)
