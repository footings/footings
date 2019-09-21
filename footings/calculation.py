
from functools import wraps, partial

from .function import _BaseFunction


class Calculation(_BaseFunction):
    """
    A class to identify calculations specifically built for the footings framework.
    """
    def __init__(self, function, output_attrs: dict = None):
        if output_attrs is None:
            output_attrs = {'ftype': Calculation}
        else:
            assert 'ftype' not in output_attrs, 'ftype not allowed as output_attrs'
            output_attrs.update({'ftype': Calculation})
        super().__init__(function, output_attrs)


# inspiration from 
# https://stackoverflow.com/questions/10294014/python-decorator-best-practice-using-a-class-vs-a-function
def as_calculation(function=None, **kwargs):
    if not function:
        return partial(Calculation, **kwargs)
    @wraps(function)
    def wrapper(function):
         return Calculation(function, **kwargs)
    return wrapper(function)
