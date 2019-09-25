from functools import wraps, partial

from .function import _BaseFunction


class AssumptionDeterministic(_BaseFunction):
    """
    A class to identify assumptions specifically built for the footings framework.
    """

    def __init__(self, function, output_attrs: dict = None):
        if output_attrs is None:
            output_attrs = {"ftype": AssumptionDeterministic}
        else:
            assert "ftype" not in output_attrs, "ftype not allowed as output_attrs"
            output_attrs.update({"ftype": AssumptionDeterministic})
        super().__init__(function, output_attrs)


# inspiration from
# https://stackoverflow.com/questions/10294014/python-decorator-best-practice-using-a-class-vs-a-function
def as_assumption_deterministic(function=None, **kwargs):
    if not function:
        return partial(AssumptionDeterministic, **kwargs)

    @wraps(function)
    def wrapper(function):
        return AssumptionDeterministic(function, **kwargs)

    return wrapper(function)


class AssumptionStochastic(_BaseFunction):
    """
    A class to identify assumptions specifically built for the footings framework.
    """

    def __init__(self, function, output_attrs: dict = None):
        if output_attrs is None:
            output_attrs = {"ftype": AssumptionStochastic}
        else:
            assert "ftype" not in output_attrs, "ftype not allowed as output_attrs"
            output_attrs.update({"ftype": AssumptionStochastic})
        super().__init__(function, output_attrs)


# inspiration from
# https://stackoverflow.com/questions/10294014/python-decorator-best-practice-using-a-class-vs-a-function
def as_assumption_stochastic(function=None, **kwargs):
    if not function:
        return partial(AssumptionStochastic, **kwargs)

    @wraps(function)
    def wrapper(function):
        return AssumptionStochastic(function, **kwargs)

    return wrapper(function)
