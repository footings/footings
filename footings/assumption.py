from functools import wraps, partial

from .function import _BaseFunction


class Assumption(_BaseFunction):
    """
    A class to identify assumptions specifically built for the footings framework.
    """

    def __init__(self, function, output_attrs: dict = None):
        if output_attrs is None:
            output_attrs = {"ftype": Assumption}
        else:
            assert "ftype" not in output_attrs, "ftype not allowed as output_attrs"
            output_attrs.update({"ftype": Assumption})
        super().__init__(function, output_attrs)


# inspiration from
# https://stackoverflow.com/questions/10294014/python-decorator-best-practice-using-a-class-vs-a-function
def as_assumption(function=None, **kwargs):
    if not function:
        return partial(Assumption, **kwargs)

    @wraps(function)
    def wrapper(function):
        return Assumption(function, **kwargs)

    return wrapper(function)
