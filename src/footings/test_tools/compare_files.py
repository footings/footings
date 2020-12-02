from functools import singledispatch
import pathlib

import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from .load_file import load_footings_file

#########################################################################################
# comparison functions
#########################################################################################


@singledispatch
def compare_values(val1, val2):
    """A dispatch function to compare two values."""
    result = val1 == val2
    if result:
        msg = ""
    else:
        msg = "The values are different when tested generically."
    return result, msg


@compare_values.register(pd.DataFrame)
def _(val1, val2):
    result = True
    msg = ""
    try:
        assert_frame_equal(val1, val2)
    except AssertionError:
        result = False
        msg = "The values are different when tested using pd.assert_frame_equal."
    return result, msg


@compare_values.register(pd.Series)
def _(val1, val2):
    result = True
    msg = ""
    try:
        assert_series_equal(val1, val2)
    except AssertionError:
        result = False
        msg = "The values are different when tested using pd.assert_series_equal."
    return result, msg


def exclude_record(key, exclude):
    return any([all([getattr(key, k) == v for k, v in e.items()]) for e in exclude])


def compare_file_dicts(result: dict, expected: dict, **kwargs):
    test = True
    log = {}
    keys = set(list(result.keys()) + list(expected.keys()))
    exclude = kwargs.pop("exclude", [])
    for key in keys:
        temp = True
        if len(exclude) > 0:
            if exclude_record(key, exclude):
                continue
        try:
            res_val = result[key]
        except KeyError:
            msg = f"The result file is missing the key [{key}] that exists in the expected file."
            temp = False

        try:
            exp_val = expected[key]
        except KeyError:
            msg = f"The expected file is missing the key [{key}] that exists in the result file."
            temp = False

        if temp:
            if type(res_val) != type(exp_val):
                temp = False
                msg = f"The value types are different at [{key}]."
            else:
                temp, msg = compare_values(res_val, exp_val)

        if temp is False:
            test = False
        log.update({key: {"result": temp, "msg": msg}})

    message = "\n".join(
        [
            f"{k} : {str(v['result'])} : {v['msg']}"
            for k, v in log.items()
            if v["result"] is False
        ]
    )
    return test, message


def assert_footings_files_equal(result: str, expected: str, **kwargs):
    """Test two files to determine if they are equal.

    This function is useful for unit testing models to ensure models stay true over time.

    Currently .json and .xlsx file extensions are supported.

    Parameters
    ----------
    result : str
        The new workbook to test against an expected workbook.
    expected : str
        The baseline workbook to compare the result against.
    **kwargs
        Additional parameters to pass.

    Returns
    -------
    bool
        True if the audit files are equal else raises AssertionError.

    Raises
    ------
    ValueError
        If the result and expected audit files share different extension types.
    AssertionError
        If the results and expected audit files are different.
    """
    result_ext = pathlib.Path(result).suffix
    expected_ext = pathlib.Path(expected).suffix

    if result_ext != expected_ext:
        msg = f"The file extensions for result [{result_ext}] and expected [{expected_ext}] do not match."
        raise ValueError(msg)

    result = load_footings_file(result)
    expected = load_footings_file(expected)

    test, message = compare_file_dicts(result=result, expected=expected, **kwargs)
    if test is False:
        raise AssertionError(f"\n{message}")
    return True
