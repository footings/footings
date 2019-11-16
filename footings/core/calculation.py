from functools import wraps, partial

from .function import _BaseFunction


class Calculation(_BaseFunction):
    """
    A class to identify calculations specifically built for the footings framework.
    """

    def __init__(self, function):
        super().__init__(function)
        self.ftype = "Calculation"


def as_calculation(function):
    @wraps(function)
    def wrapper(function):
        return Calculation(function)

    return wrapper(function)
