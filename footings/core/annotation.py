from numpydoc.docscrape import FunctionDoc
from inspect import getfullargspec
from functools import wraps, partial

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
        if default is not None:
            assert self.valid(default)
        self.default = default

    def valid(self, value):
        if self.dtype is not None:
            assert type(value) == self.dtype, _generate_message(
                "value type is not ", self.dtype
            )
        if self.allowed is not None:
            assert value in self.allowed, _generate_message(
                "value assigned is not one of ", self.allowed
            )
        return True


def parse_annotation(function, method):
    """This function takes a function and parses input and output assignments either 
    through annotations or the function docstring. If using the method of docstring "D",
    it assumes using the Numpy docstring format.
    
    Parameters
    ----------
    function : function
        The function to parse
    method : str
        Either "A" for annotation or "D" for docstring 
    
    Returns
    -------
    Dict["input_columns", "settings", "output_columns"]
        A dict with keys for - 
        - the input columns into a function
        - the settings for a function
        - the return columns of a function (i.e., the output columns)
    
    Examples
    --------
    >>> from footings import Frame, FReturn, as_calculation, parse_annotation
    >>> @as_calculation
        def calc_v(df: Frame({"i": "float"})) -> FReturn({"v": "float"}):
            return df.assign(v=1 / (1 + df.i))
    >>> parse_annotation(calc_v, method="A")

    See Also
    --------
    https://numpydoc.readthedocs.io/en/latest/format.html

    """
    # parse docstring or annotation based on method
    args = set(getfullargspec(function).args)
    if method == "A":
        d_anno = function.__annotations__.copy()
        d_out = d_anno["return"]
        d_in = {k: v for k, v in d_anno.items() if k != "return"}
    elif method == "D":
        doc = FunctionDoc(function)
        assert len(doc["Returns"]) == 1, "Return object from docstring must be length 1"
        d_out = eval(doc["Returns"][0].type)
        d_in = {x.name: eval(x.type) for x in doc["Parameters"]}
    else:
        raise AssertionError("method be either 'A' or 'D' not [" + method + "]")

    def validate_input_output(args, d_in, d_out):
        anno = set(d_in.keys())
        items = d_in.items()

        # validate no parameters not annotated
        diff_args = args.difference(anno)
        if len(diff_args) > 0:
            msg = "The following function parameters need to be annotated - "
            raise AssertionError(_generate_message(msg, diff_args))

        # validate input annotated types
        unk_types = [k for k, v in items if type(v) not in [Column, Frame, Setting]]
        if len(unk_types) > 0:
            msg = "The following function parameters are assigned invalid input types - "
            raise AssertionError(_generate_message(msg, unk_types))

        # validate instances
        not_inste = [
            k
            for k, v in items
            if any([isinstance(v, i) for i in [Column, Frame, Setting]]) == False
        ]
        if len(not_inste) > 0:
            msg = (
                "The following function parameters are assigned non instantiated types - "
            )
            raise AssertionError(_generate_message(msg, not_inste))

        # validate input combinations
        c = [k for k, v in items if isinstance(v, Column)]
        f = [k for k, v in items if isinstance(v, Frame)]
        if len(c) > 0 and len(f) > 0:
            msg = "Column and Frame types cannot be used together as annotation inputs."
            raise AssertionError(msg)

        # validate input/return combinations
        if len(c) > 0 and isinstance(d_out, CReturn) == False:
            msg = (
                "If using Columns as input, return needs to be annotated with "
                "an instance of CReturn."
            )
            raise AssertionError(msg)

        if len(f) > 0 and isinstance(d_out, FReturn) == False:
            msg = (
                "If using a Frame as input, return needs to be annotated with "
                "an instance of FReturn."
            )
            raise AssertionError(msg)

        return True

    # validate annotations
    assert validate_input_output(args, d_in, d_out)

    # parse assignments into input_columns, settings, and output_columns to be returned
    input_columns = {}
    for k, v in d_in.items():
        if type(v) == Column:
            input_columns.update({k: v.dtype})
        elif type(v) == Frame:
            for c, t in v.columns.items():
                input_columns.update({c: t})

    settings = {k: v for k, v in d_in.items() if type(v) == Setting}
    output_columns = {k: v for k, v in d_out.columns.items()}

    return {
        "input_columns": input_columns,
        "settings": settings,
        "output_columns": output_columns,
    }
