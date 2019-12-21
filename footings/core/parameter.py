from .utils import _generate_message


class Parameter:
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
