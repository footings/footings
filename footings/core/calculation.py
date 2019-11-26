from functools import wraps, partial
from toolz import curry

from .function import _BaseFunction


class Calculation(_BaseFunction):
    """
    A class to identify calculations specifically built for the footings framework.
    """

    def __init__(self, function, method):
        super().__init__(function, method)
        self.ftype = "Calculation"


@curry
def as_calculation(function, method):
    return Calculation(function, method)
