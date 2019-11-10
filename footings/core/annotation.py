from .utils import _generate_message

_allowed_dtypes = [
    "object",
    "str",
    "int",
    "int64",
    "float",
    "float64",
    "bool",
    "datetime64",
    "category",
    float,
    int,
    str,
]


class ColDict:
    """ 
    Base class for CReturn, Frame, FReturn
    """

    def __init__(self, col_dict: dict):
        wrong_types = [k for k, v in col_dict.items() if v not in _allowed_dtypes]
        if len(wrong_types) > 0:
            msg = "The following keys in col_dict were assign invalid dtypes "
            raise AssertionError(_generate_message(msg, wrong_types))
        self.columns = col_dict
        self.nodes = list(col_dict.keys())


class Column:
    """ The class used to represent a column input in the task graph that is built under 
    the footings framework. It is used to annotate input columns in functions.

    Parameters
    ----------
    dtype : 
        The data type of the column. Allows any data type allowed under Pandas.

    Examples
    --------
    >>> from footings import Column, CReturn, as_calculation
    >>> @as_calculation
        def calc_v(i: Column("float")) -> CReturn({"v": "float"}):
            return 1 / (1 + i)
    
    See Also
    --------
    https://pandas.pydata.org/pandas-docs/stable/getting_started/basics.html#basics-dtypes

    """

    def __init__(self, dtype):
        assert dtype in _allowed_dtypes, _generate_message(
            "Column was assigned an invalide dtype ", [str(dtype)]
        )
        self.dtype = dtype


class CReturn(ColDict):
    """ The class used to represent a column output in the task graph that is built under 
    the footings framework. It is used to annotate return columns in functions.

    Parameters
    ----------
    col_dict : Dict[str, str]
        A key value pair where k: v where k is equal to the column name and v is dtype. 
        Only one pair is allowed (i.e., only one column can be returned).
        Allows any data type allowed under Pandas.

    Examples
    --------
    >>> from footings import Column, CReturn, as_calculation
    >>> @as_calculation
        def calc_v(i: Column("float")) -> CReturn({"v": "float"}):
            return 1 / (1 + i)
    
    See Also
    --------
    https://pandas.pydata.org/pandas-docs/stable/getting_started/basics.html#basics-dtypes

    """

    def __init__(self, col_dict: dict):
        assert len(col_dict) == 1, "CReturn cannot return multiple columns"
        super().__init__(col_dict=col_dict)


class Frame(ColDict):
    """ The class used to represent a frame input in the task graph that is built under 
    the footings framework. It is used to annotate input frames in functions.

    Parameters
    ----------
    col_dict : Dict[str, str]
        A key value pair where k: v where k is equal to the column name and v is dtype. 
        Allows mutiple pairs (i.e., multiple columns can be used as input to a function).
        Allows any data type allowed under Pandas.

    Examples
    --------
    >>> from footings import Frame, FReturn, as_calculation
    >>> @as_calculation
        def calc_v(df: Frame({"i": "float"})) -> FReturn({"v": "float"}):
            return df.assign(v=1 / (1 + df.i))
    
    See Also
    --------
    https://pandas.pydata.org/pandas-docs/stable/getting_started/basics.html#basics-dtypes

    """

    def __init__(self, col_dict: dict):
        super().__init__(col_dict=col_dict)


class FReturn(ColDict):
    """ The class used to represent a frame output in the task graph that is built under 
    the footings framework. It is used to annotate return frames in functions.

    Parameters
    ----------
    col_dict : Dict[str, str]
        A key value pair where k: v where k is equal to the column name and v is dtype. 
        Allows mutiple pairs (i.e., multiple columns can be used as output from a function).
        Allows any data type allowed under Pandas.

    Examples
    --------
    >>> from footings import Frame, FReturn, as_calculation
    >>> @as_calculation
        def calc_v(df: Frame({"i": "float"})) -> FReturn({"v": "float"}):
            return df.assign(v=1 / (1 + df.i))
    
    See Also
    --------
    https://pandas.pydata.org/pandas-docs/stable/getting_started/basics.html#basics-dtypes

    """

    def __init__(self, col_dict: dict):
        super().__init__(col_dict=col_dict)


class Setting:
    """ The class used to represent a model setting in the task graph that is built under 
    the footings framework. It is used to annotate Settings in functions.

    Parameters
    ----------
    dtype: 
        Optional. The data type of the setting. Not limited to Pandas dtypes.

    allowed: 
        Optional. A list of allowed values.

    default:
        Optional. A default value.

    Examples
    --------
    >>> from footings import Column, CReturn, Setting, as_calculation
    >>> @as_calculation
        def calc_v(
            i: Column("float"), 
            mode: Setting(dtype=str, allowed=["M", "A"])
        ) -> CReturn({"v": "float"}):
            return 1 / (1 + i)
    
    See Also
    --------


    """

    def __init__(self, dtype=None, allowed=None, default=None):
        self.dtype = dtype
        self.allowed = allowed
        self.default = default

    def validate(self, value):
        if self.dtype is not None:
            assert type(value) == self.dtype, _generate_message(
                "value type is not ", self.dtype
            )
        if self.allowed is not None:
            assert value in self.allowed, _generate_message(
                "value assigned is not one of ", self.allowed
            )


def _parse_annotation_settings(x):
    return {k: v for k, v in x.__annotations__.items() if type(v) == Setting}


def _parse_annotation_input(x):
    if type(x.__annotations__["return"]) == CReturn:
        return {k: v.dtype for k, v in x.__annotations__.items() if type(v) == Column}
    elif type(x.__annotations__["return"]) == FReturn:
        l = []
        for k, v in x.__annotations__.items():
            if type(v) == Frame:
                l.append(v.columns)
        assert len(l) == 1, "check frame"
        return l[0]


def _parse_annotation_output(x):
    return x.__annotations__["return"].columns
