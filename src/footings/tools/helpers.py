from typing import Callable, List
from functools import partial

from footings import loaded_function


def post_drop_columns(columns: List[str], function: Callable = None):
    """
    A loaded function that can be used to drop column from a table.

    This function is useful when you want to break up a complex formula in a DataFrame across
    multiple columns for audit purposes even though only the final result is needed. This can
    be added to the function to drop the not needed columns at the end.

    Parameters
    ----------
    function : callable
        The function that returns the DataFrame you want to drop columns from.
    columns : list of strings
        The list of columns to drop.

    Returns
    -------
    footings.core.utils.LoadedFunction
        A loaded function with the function to drop columns added to the registry.

    See Also
    --------
    footings.core.utils.LoadedFunction

    Examples
    --------
    >>> import pandas as pd
    >>> from footings.library import post_drop_columns
    >>>
    >>> @post_drop_columns(columns=["col_1", "col_2"])
    >>> def create_frame():
    >>>     df = pd.DataFrame(
    >>>         {
    >>>             "col_1": [1, 2, 3],
    >>>             "col_2": [4, 5, 6],
    >>>             "col_3": [7, 8, 9]
    >>>         }
    >>>     )
    >>>     return df
    >>> create_frame()
    >>> #	    col_3
    >>> # 0	    7
    >>> # 1	    8
    >>> # 2	    9
    """
    if function is None:
        return partial(post_drop_columns, columns)

    if hasattr(function, "loaded") is False:
        function = loaded_function(function=function)

    @function.register(position="end")
    def drop_columns(tbl):
        return tbl.drop(columns, axis=1)

    return function
