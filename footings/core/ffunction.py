import pandas as pd
from pyarrow import Field
from collections import namedtuple
from typing import Optional, Dict, List

from .parameter import Parameter


def to_dataframe_function(
    function: callable,
    input_columns: List[str],
    output_columns: List[Field],
    parameters: Optional[Dict[str, Parameter]] = None,
) -> callable:
    """A function that transforms a function that returns a pandas series to return a \
    panda dataframe.

    Parameters
    ----------
    function : callable
        The function to transform.
    input_columns : list
        The names of the columns used in the function.
    output_columns : list
        A list of fields returned from the function.
    parameters : dict, optional
        The parameters defined within the function.

    Returns
    -------
    callable
        A callable function that takes in dataframe and returns a dataframe when called.

    Examples
    --------
    >>> import pandas as pd
    >>> import pyarrow as pa
    >>> from footings import Parameter

    >>> df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    >>> def add(a, b):
            return a + b

    >>> def add_subtract(a, b):
            return a + b, b - a

    >>> add_df_func = to_dataframe_function(
            add,
            input_columns=["a", "b"],
            output_columns=[pa.field("add", pa.int16())],
        )

    >>> add_df_func(df)

    >>> add_subtract_df_func = to_dataframe_function(
            add_subtract,
            input_columns=["a", "b"],
            output_columns=[
                pa.field("add", pa.int16()), pa.field("subtract", pa.int16())
            ],
        )

    >>> add_subtract_df_func(df)

    """
    if parameters is None:
        parameters = {}

    # need to test input_columns and output_columns
    def df_function(function, input_columns, output_columns, parameters):

        ret = [f.name for f in output_columns]

        def wrapper(_df, **parameters):
            def exp(x):
                return function(**{k: x[k] for k in input_columns}, **parameters)

            if len(ret) == 1:
                _df = _df.assign(**{ret[0]: exp(_df)})
            else:
                _df = _df.assign(**dict(zip(ret, exp(_df))))
            return _df

        return wrapper

    return df_function(function, input_columns, output_columns, parameters)


class FFunction:
    """ A callable object (i.e., function) fitted for use within the Footings framework.

    Parameters
    ----------
    function : callable
        The function to transform to a FFunction.
    return_type : {pd.Series, pd.DataFrame}
        The return type of the function. Can be either pd.Series or pd.DataFrame.
    input_columns : list
        The names of the columns used in the function.
    output_columns : list
        A list of fields returned from the function.
    parameters : dict, optional
        A dict with {"function argument": Parameter(..), ..} of the parameters used in \
        the function. Allows also passing an empty dict (i.e., {}).
    drop_columns : list, optional
        A list of column names to drop from the dataframe. Note that all the column \
        names must be specified as input_columns.

    Returns
    -------
    FFunction
        A callable object fitted for use within the Footings framework.

    Examples
    --------
    >>> import pandas as pd
    >>> import pyarrow as pa
    >>> from footings import Parameter

    >>> df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    >>> def func(a, b, s):
            if s == "leave":
                return a + b
            else:
                return (a + b) / 2

    >>> s_func = FFunction(
            func,
            return_type=pd.Series,
            input_columns=["a", "b"],
            parameters={"s": Parameter(allowed=["leave", "divide"])},
            output_columns={"c": pa.float64()},
        )

    >>> print(s_func)

    >>> def df_add(df):
            return df.assign(c=df.a + df.b)

    >>> df_func = FFunction(
            add1,
            return_type=pd.DataFrame,
            input_columns=["a", "b"],
            output_columns=[pa.field("c", pa.int16())],
        )

    >>> print(df_func)
    """

    def __init__(
        self,
        function: callable,
        return_type: type,
        input_columns: List[str],
        output_columns: List[Field],
        parameters: Optional[Dict[str, Parameter]] = None,
        drop_columns: Optional[List[str]] = None,
    ):

        self._function = function
        self._return_type = self._validate_return_type(return_type)
        self._input_columns = self._validate_input_columns(input_columns)
        self._output_columns = self._validate_output_columns(output_columns)
        self._parameters = self._validate_parameters(parameters)
        self._drop_columns = self._validate_drop_columns(
            drop_columns, return_type, input_columns
        )

        if return_type == pd.Series:
            self._ffunction = to_dataframe_function(
                function=function,
                input_columns=self._input_columns,
                output_columns=self._output_columns,
                parameters=self._parameters,
            )
        else:
            self._ffunction = function

    @property
    def function(self):
        return self._function

    @property
    def return_type(self):
        return self._return_type

    @property
    def name(self):
        return self._function.__name__

    @property
    def input_columns(self):
        return self._input_columns

    @property
    def parameters(self):
        return self._parameters

    @property
    def drop_columns(self):
        return self._drop_columns

    @property
    def output_columns(self):
        return self._output_columns

    def generate_step(self):
        """Generates a Step (i.e., a namedtuple object) that has the neccessary \
        information to be used within a model in the Footings framework.
        """
        nms = [
            "cls",
            "name",
            "input_columns",
            "parameters",
            "output_columns",
            "drop_columns",
            "function",
        ]

        Step = namedtuple("Step", nms)
        return Step(
            cls=self.__class__.__name__,
            name=self.name,
            input_columns=self.input_columns,
            parameters=self.parameters,
            output_columns=self.output_columns,
            drop_columns=self.drop_columns,
            function=self._ffunction,
        )

    @staticmethod
    def _validate_return_type(return_type):
        if return_type not in [pd.Series, pd.DataFrame]:
            raise ValueError(
                "return_type must be either pd.Series or pd.DataFrame not {0}".format(
                    return_type
                )
            )
        return return_type

    @staticmethod
    def _validate_dict(d, value_type, allow_empty_dict=True, allow_none=False):
        if allow_none is True and d is None:
            pass
        else:
            if isinstance(d, dict) is False:
                raise TypeError("{0} must be a dict".format(d))
            if allow_empty_dict is False:
                if d == {}:
                    raise ValueError("{0} cannot be an empty dict".format(d))
            if d != {}:
                for k, v in d.items():
                    if type(v) is not value_type:
                        raise TypeError(
                            "The value for {0} must be a {1} not {2}".format(
                                k, value_type, type(v)
                            )
                        )
        return d

    @staticmethod
    def _validate_input_columns(input_columns):
        if isinstance(input_columns, list) is False:
            raise TypeError("input_columns must be a list")
        return input_columns

    def _validate_parameters(self, parameters):
        return self._validate_dict(parameters, Parameter, allow_none=True)

    @staticmethod
    def _validate_output_columns(output_columns):
        if isinstance(output_columns, list) is False:
            raise TypeError("output_columns must be a list")
        if any([type(f) != Field for f in output_columns]):
            raise TypeError(
                f"All values passed to output_columns must be of type {Field}"
            )
        return output_columns

    @staticmethod
    def _validate_drop_columns(drop_columns, return_type, input_columns):
        if drop_columns is None:
            return None
        else:
            if type(drop_columns) != list:
                raise TypeError("drop_columns must be passed as a list.")

            if return_type == pd.Series and drop_columns is not None:
                raise ValueError(
                    """When return_type is pd.Series, drop_columns must be None."""
                )

            in_cols = set(input_columns)
            dp_cols = set(drop_columns)
            if len(dp_cols - in_cols) > 0:
                raise ValueError(
                    "All columns in drop_columns must be present in input_columns."
                )
            return drop_columns

    # need name mapper
    def __call__(self, *args, **kwargs):
        return self._ffunction(*args, **kwargs)

    def __repr__(self):
        s = """FFunction -
        id : {id}
        name : {name}
        input_columns : {input_columns}
        parameters : {parameters}
        output_columns : {output_columns}
        drop_columns : {drop_columns}
        """.format(
            id=id(self),
            name=self.name,
            input_columns=self.input_columns,
            parameters=self.parameters,
            output_columns=self.output_columns,
            drop_columns=self.drop_columns,
        )
        return s


def ffunction(
    *,
    return_type: type,
    input_columns: List[str],
    output_columns: List[Field],
    parameters: Optional[Dict[str, Parameter]] = None,
    drop_columns: Optional[List[str]] = None,
):
    """A decorator that can be used to create a FFunction object.

    Parameters
    ----------
    function : callable
        The function to transform to a FFunction.
    return_type : {pd.Series, pd.DataFrame}
        The return type of the function. Can be either pd.Series or pd.DataFrame.
    input_columns : list
        The names of the columns used in the function.
    output_columns : list
        A list of fields returned from the function.
    parameters : dict, optional
        A dict with {"function argument": Parameter(..), ..} of the parameters used in \
        the function. Allows also passing an empty dict (i.e., {}).
    drop_columns : list, optional
        A list of column names to drop from the dataframe. Note that all the column \
        names must be specified as input_columns.

    Returns
    -------
    FFunction
        A callable object fitted for use within the Footings framework.

    See Also
    --------
    FFunction : the class returned

    Examples
    --------
    >>> import pandas as pd
    >>> import pyarrow as pa
    >>> from footings import Parameter

    >>> df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    >>> @ffunction(
            return_type=pd.Series,
            input_columns=["a", "b"],
            output_columns=[pa.field("c", pa.int16())],
        )
        def add(a, b):
            return a + b

    >>> add(df)

    """

    def inner(function):
        def wrapped_func():
            return FFunction(
                function=function,
                return_type=return_type,
                input_columns=input_columns,
                output_columns=output_columns,
                parameters=parameters,
                drop_columns=drop_columns,
            )

        return wrapped_func()

    return inner


def series_ffunction(
    *,
    input_columns: List[str],
    output_columns: List[Field],
    parameters: Optional[Dict[str, Parameter]] = None,
    drop_columns: Optional[List[str]] = None,
):
    """A decorator that can be used to create a FFunction object where return_type \
    is pd.Series.

    Parameters
    ----------
    function : callable
        The function to transform to a FFunction.
    input_columns : list
        The names of the columns used in the function.
    output_columns : list
        A list of fields returned from the function.
    parameters : dict, optional
        A dict with {"function argument": Parameter(..), ..} of the parameters used in \
        the function. Allows also passing an empty dict (i.e., {}).
    drop_columns : list, optional
        A list of column names to drop from the dataframe. Note that all the column \
        names must be specified as input_columns.

    Returns
    -------
    FFunction
        A callable object fitted for use within the Footings framework.

    See Also
    --------
    FFunction : the class returned

    Examples
    --------
    >>> import pandas as pd
    >>> import pyarrow as pa
    >>> from footings import Parameter

    >>> df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    >>> @series_ffunction(
            input_columns=["a", "b"],
            output_columns=[pa.field("c", pa.int16())],
        )
        def add(a, b):
            return a + b

    >>> add(df)

    """

    def inner(function):
        def wrapped_func():
            return FFunction(
                function=function,
                return_type=pd.Series,
                input_columns=input_columns,
                output_columns=output_columns,
                parameters=parameters,
                drop_columns=drop_columns,
            )

        return wrapped_func()

    return inner


def dataframe_ffunction(
    *,
    input_columns: List[str],
    output_columns: List[Field],
    parameters: Optional[Dict[str, Parameter]] = None,
    drop_columns: Optional[List[str]] = None,
):
    """A decorator that can be used to create a FFunction object where return_type \
    is pd.DataFrame.

    Parameters
    ----------
    function : callable
        The function to transform to a FFunction.
    input_columns : list
        The names of the columns used in the function.
    output_columns : list
        A list of fields returned from the function.
    parameters : dict, optional
        A dict with {"function argument": Parameter(..), ..} of the parameters used in \
        the function. Allows also passing an empty dict (i.e., {}).
    drop_columns : list, optional
        A list of column names to drop from the dataframe. Note that all the column \
        names must be specified as input_columns.

    Returns
    -------
    FFunction
        A callable object fitted for use within the Footings framework.

    See Also
    --------
    FFunction : the class returned

    Examples
    --------
    >>> import pandas as pd
    >>> import pyarrow as pa
    >>> from footings import Parameter

    >>> df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    >>> @dataframe_ffunction(
            input_columns=["a", "b"],
            output_columns=[pa.field("c", pa.int16())],
        )
        def add(df):
            return df.assign(c=df.a + df.b)

    >>> add(df)

    """

    def inner(function):
        def wrapped_func():
            return FFunction(
                function=function,
                return_type=pd.DataFrame,
                input_columns=input_columns,
                output_columns=output_columns,
                parameters=parameters,
                drop_columns=drop_columns,
            )

        return wrapped_func()

    return inner
