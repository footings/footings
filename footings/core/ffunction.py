import pandas as pd
from pyarrow import Field
from collections import namedtuple
from typing import Optional, Dict, List
from collections import namedtuple
from toolz import curry

from .parameter import Parameter
from .errors import InputsTypeError, InputsValueError, OutputsTypeError, OutputsValueError

DFIn = namedtuple("DFIn", ["name", "required_columns"])
DFOut = namedtuple(
    "DFOut", ["name", "added_columns", "dropped_columns"], defaults=[None, None, None]
)


def to_dataframe_function(function: callable, inputs, outputs) -> callable:
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
    if isinstance(inputs, list):
        dfs_in = []
        parameters = []
        for i in inputs:
            if type(i) == DFIn:
                dfs_in.append(i)
            elif type(i) == Parameter:
                parameters.append(i)
            else:
                raise InputsTypeError(
                    f"The object {i} passed to inputs has {type(i)} and is not \
                        allowed."
                )
        if len(dfs_in) > 1:
            InputsValueError(
                "Cannot pass more than one DFIn object into inputs \
                    when return_type == pd.Series."
            )
        elif len(dfs_in) == 0:
            input_columns = []
        else:
            df = dfs_in[0].name
            input_columns = dfs_in[0].required_columns
    elif isinstance(inputs, DFIn):
        df = inputs.name
        input_columns = inputs.required_columns
        parameters = []
    elif isinstance(inputs, Parameter):
        input_columns = []
        parameters = [inputs]
    else:
        raise InputsTypeError(
            f"The object passed to inputs has type {type(inputs)} \
                and is not allowed."
        )

    if isinstance(outputs, list):
        if isinstance(outputs, DFOut) and len(outputs) == 1:
            output_columns = outputs[0].added_columns
        elif isinstance(outputs, DFOut) and len(outputs) > 1:
            raise OutputsValueError(
                "Cannot pass more than one DFOut object into outputs \
                    when return_type == pd.Series."
            )
        else:
            raise OutputsTypeError(
                f"The object passed to outputs has type {type(outputs)} \
                    and is not allowed."
            )
    elif isinstance(outputs, DFOut):
        output_columns = outputs.added_columns
    else:
        raise OutputsTypeError(
            f"The object passed to outputs has type {type(outputs)} \
                and is not allowed."
        )

    if parameters is [] or parameters is None:
        parameters = {}
    else:
        parameters = (p.name for p in parameters)

    def df_function(function, input_columns, output_columns, parameters):

        ret = [f[0] for f in output_columns]
        # replace_sig = (inputs.name, *parameters)
        def wrapper(_df, *args, **kwargs):
            def exp(x):
                return function(**{k: x[k].values for k in input_columns}, **kwargs)

            if len(ret) == 1:
                return _df.assign(**{ret[0]: exp(_df)})
            else:
                return _df.assign(**dict(zip(ret, exp(_df))))
            return _df

        # sig = signature(wrapper)
        # wrapper.__signature__ = sig.replace(parameters=tuple())
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
        self, function: callable, return_type: type, inputs, outputs,
    ):

        self._function = function
        self._return_type = self._validate_return_type(return_type)
        self._inputs = inputs
        self._outputs = outputs
        self._input_validator = self._create_validator(inputs)
        self._output_validator = self._create_validator(outputs)

        if return_type == pd.Series:

            self._ffunction = to_dataframe_function(
                function=function, inputs=inputs, outputs=outputs,
            )
        else:
            self._ffunction = function

    @staticmethod
    def parse_inputs(inputs):
        input_columns = []
        parameters = []
        for i in inputs:
            if type(i) == DFIn:
                input_columns += i.input_columns
            elif type(i) == Parameter:
                parameters.append(i)
            else:
                raise
        return input_columns, parameters

    @staticmethod
    def parse_outputs(outputs):
        return outputs.output_columns, outputs.drop_columns

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
    def inputs(self):
        return self._inputs

    @property
    def outputs(self):
        return self._outputs

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
    def _create_validator(x, *args, **kwargs):
        pass

    # need name mapper / need option to turn off validations
    def __call__(self, *args, **kwargs):
        # self._input_validator(self.inputs, *arg, **kwargs)
        out = self._ffunction(*args, **kwargs)
        # self._output_validator(self.outputs, out)
        return out

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
    *, return_type: type, inputs, outputs,
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
                inputs=inputs,
                outputs=outputs,
            )

        return wrapped_func()

    return inner


def series_ffunction(
    *, inputs, outputs,
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
                function=function, return_type=pd.Series, inputs=inputs, outputs=outputs,
            )

        return wrapped_func()

    return inner


def dataframe_ffunction(
    *, inputs, outputs,
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
                inputs=inputs,
                outputs=outputs,
            )

        return wrapped_func()

    return inner
