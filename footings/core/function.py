import pandas as pd
from pyarrow import DataType
from collections import namedtuple
from toolz import curry
from typing import Optional, Dict, Union, List

from .annotation import Setting
from .utils import _generate_message


def ff_function(
    function: callable,
    return_type: type,
    input_columns: Optional[Union[Dict[str, str], Dict[str, DataType]]] = None,
    settings: Optional[Dict[str, Setting]] = None,
    output_columns: Optional[Union[Dict[str, str], Dict[str, DataType]]] = None,
    name_mapping: Optional[Union[str, Dict[str, str]]] = None,
) -> callable:
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

    if return_type == pd.Series:
        return df_function(function, input_columns, output_columns, settings)
    elif return_type == pd.DataFrame:
        return function
    else:
        raise ValueError(
            "The value for return_type must be {0} or {1}".format(pd.Series, pd.DataFrame)
        )


class FFunction:
    """
    A class to identify functions specifically built for the footings framework.
    """

    def __init__(
        self,
        function: callable,
        return_type: type,
        parse_args: bool = True,
        input_columns: Optional[Union[Dict[str, str], Dict[str, DataType]]] = None,
        settings: Optional[Dict[str, Setting]] = None,
        drop_columns: Optional[List[str]] = None,
        output_columns: Optional[Union[Dict[str, str], Dict[str, DataType]]] = None,
        name_mapping: Optional[Union[str, Dict[str, str]]] = None,
    ):

        assert (
            drop_columns is not None or output_columns is not None
        ), "Both drop_columns and output_columns cannot be None when building a FFunction."

        self._function = function
        self._output_columns = output_columns
        self._drop_columns = drop_columns

        if parse_args:
            if return_type == pd.Series:
                if input_columns is not None and settings is not None:
                    raise ValueError(
                        "Both input_columns and settings have to be None when parse_args \
                        is True."
                    )
                self._input_columns = {
                    k: v
                    for k, v in function.__annotations__.items()
                    if type(v) == DataType
                }
            if return_type == pd.DataFrame:
                if input_columns is None:
                    raise ValueError(
                        "When parse_args is True and the return_type is pd.DataFrame the \
                        value for input_columns cannot be None."
                    )
                self._input_columns = input_columns
            self._settings = {
                k: v for k, v in function.__annotations__.items() if type(v) == Setting
            }
        else:
            if input_columns is None and settings is None:
                raise ValueError(
                    "Both input_columns and settings cannot be None when parse_args is \
                    False."
                )
            self._input_columns = input_columns
            self._settings = settings

        self._ffunction = ff_function(
            function,
            return_type,
            self._input_columns,
            self._settings,
            self._output_columns,
            name_mapping,
        )

    @property
    def function(self):
        return self._function

    @property
    def name(self):
        return self._function.__name__

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

    # need name mapper
    def __call__(self, *args, **kwargs):
        return self._ffunction(*args, **kwargs)

    # def __repr__(self):
    #     pass


@curry
def ffunction(
    function: callable,
    return_type: type,
    parse_args: bool,
    input_columns=None,
    settings=None,
    drop_columns=None,
    output_columns=None,
    name_mapping=None,
):
    return FFunction(
        function,
        return_type,
        parse_args,
        input_columns,
        settings,
        drop_columns,
        output_columns,
        name_mapping,
    )


@curry
def column_ffunction(
    function: callable, drop_columns=None, output_columns=None, name_mapping=None
):
    return FFunction(
        function=function,
        return_type=pd.Series,
        parse_args=True,
        input_columns=None,
        settings=None,
        drop_columns=drop_columns,
        output_columns=output_columns,
        name_mapping=name_mapping,
    )


@curry
def dataframe_ffunction(
    function: callable,
    input_columns: Optional[Union[Dict[str, str], Dict[str, DataType]]] = None,
    drop_columns=None,
    output_columns=None,
    name_mapping=None,
):
    return FFunction(
        function=function,
        return_type=pd.DataFrame,
        parse_args=True,
        input_columns=input_columns,
        settings=None,
        drop_columns=drop_columns,
        output_columns=output_columns,
        name_mapping=name_mapping,
    )
