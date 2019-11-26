from functools import wraps, partial
from toolz import curry

from .function import _BaseFunction


class AssumptionDeterministic(_BaseFunction):
    """
    A class to identify assumptions specifically built for the footings framework.
    """

    def __init__(self, function, method):
        super().__init__(function, method)
        self.ftype = "AssumptionDeterministic"


@curry
def as_assumption_deterministic(function, method):
    return AssumptionDeterministic(function, method)


class AssumptionStochastic(_BaseFunction):
    """
    A class to identify assumptions specifically built for the footings framework.
    """

    def __init__(self, function, method):
        super().__init__(function, method)
        self.ftype = "AssumptionStochastic"


@curry
def as_assumption_stochastic(function, method):
    return AssumptionStochastic(function, method)
