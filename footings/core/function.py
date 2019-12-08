import pandas as pd
from collections import namedtuple
from toolz import curry

from .annotation import Column, CReturn, Frame, FReturn, Setting, parse_annotation
from .utils import _generate_message


def ff_function(function, input_columns, output_columns, settings):
    """[summary]
    
    Parameters
    ----------
    function : [type]
        [description]
    input_columns : [type]
        [description]
    output_columns : [type]
        [description]
    settings : [type]
        [description]
    
    Returns
    -------
    [type]
        [description]
    """

    def df_function(function, input_columns, output_columns, settings):
        assert len(output_columns) == 1, "output_columns can only be length 1"
        ret = list(output_columns.keys())[0]

        def wrapper(_df, **settings):
            exp = lambda x: function(
                **{k: x[k] for k in input_columns.keys()}, **settings
            )
            _df = _df.assign(**{ret: exp})
            return _df

        wrapper.__doc__ = function.__doc__
        return wrapper

    if type(function.__annotations__["return"]) == CReturn:
        return df_function(function, input_columns, output_columns, settings)
    else:
        return function


class BaseFunction:
    """
    A class to identify functions specifically built for the footings framework.
    """

    def __init__(self, function, method, output_attrs={}):

        annotations = parse_annotation(function, method)

        self._function = function
        self._name = function.__name__
        self._input_columns = annotations.get("input_columns")
        self._settings = annotations.get("settings")
        self._output_columns = annotations.get("output_columns")
        self._drop_columns = annotations.get("drop_columns")
        self._ff_function = ff_function(
            function, self.input_columns, self.output_columns, self.settings
        )

    @property
    def function(self):
        return self._function

    @property
    def name(self):
        return self._name

    @property
    def input_columns(self):
        return self._input_columns

    @property
    def settings(self):
        return self._settings

    @property
    def drop_columns(self):
        return self._drop_columns

    @property
    def output_columns(self):
        return self._output_columns

    def generate_step(self):
        nms = [
            "cls",
            "name",
            "input_columns",
            "settings",
            "output_columns",
            "drop_columns",
            "function",
        ]

        Step = namedtuple("Step", nms)
        return Step(
            cls=self.__class__.__name__,
            name=self.name,
            input_columns=self.input_columns,
            settings=self.settings,
            output_columns=self.output_columns,
            drop_columns=self.drop_columns,
            function=self._ff_function,
        )

    def __call__(self, *args, **kwargs):
        return self._function(*args, **kwargs)

    # def __repr__(self):
    #     pass


class Assumption(BaseFunction):
    """
    A class to identify assumptions specifically built for the footings framework.
    """

    def __init__(self, function, method):
        super().__init__(function, method)


@curry
def as_assumption(function, method):
    return Assumption(function, method)


class Calculation(BaseFunction):
    """
    A class to identify calculations specifically built for the footings framework.
    """

    def __init__(self, function, method):
        super().__init__(function, method)


@curry
def as_calculation(function, method):
    return Calculation(function, method)
