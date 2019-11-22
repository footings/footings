from functools import wraps, partial

from .function import _BaseFunction


class AssumptionDeterministic(_BaseFunction):
    """
    A class to identify assumptions specifically built for the footings framework.
    """

    def __init__(self, function):
        super().__init__(function)
        self.ftype = "AssumptionDeterministic"


def as_assumption_deterministic(function):
    @wraps(function)
    def wrapper(function):
        return AssumptionDeterministic(function)

    return wrapper(function)


class AssumptionStochastic(_BaseFunction):
    """
    A class to identify assumptions specifically built for the footings framework.
    """

    def __init__(self, function):
        super().__init__(function)
        self.ftype = "AssumptionStochastic"


# inspiration from
# https://stackoverflow.com/questions/10294014/python-decorator-best-practice-using-a-class-vs-a-function
def as_assumption_stochastic(function):
    @wraps(function)
    def wrapper(function):
        return AssumptionStochastic(function)

    return wrapper(function)
