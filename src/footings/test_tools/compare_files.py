from fnmatch import fnmatch
from functools import singledispatch
import pathlib
from typing import Optional

import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from footings import dispatch_function

from .load_file import load_footings_json_file, load_footings_xlsx_file
from ..to_xlsx import FootingsXlsxEntry

#########################################################################################
# comparison functions
#########################################################################################


@singledispatch
def compare_values(val1, val2, tolerance=None):
    """A dispatch function to compare two values."""
    result = val1 == val2
    if result:
        msg = ""
    else:
        msg = "The values are different when tested generically."
    return result, msg


@compare_values.register(pd.DataFrame)
def _(val1, val2, tolerance=None):
    result = True
    msg = ""
    try:
        assert_frame_equal(val1, val2)
    except AssertionError:
        result = False
        msg = "The values are different when tested using pd.assert_frame_equal."
    return result, msg


@compare_values.register(pd.Series)
def _(val1, val2, tolerance=None):
    result = True
    msg = ""
    try:
        assert_series_equal(val1, val2)
    except AssertionError:
        result = False
        msg = "The values are different when tested using pd.assert_series_equal."
    return result, msg


def make_key_checker(exclude_keys: Optional[list] = None) -> callable:
    if exclude_keys is None:
        return lambda key: False

    def key_checker(key):
        if isinstance(key, FootingsXlsxEntry):
            key = key.mapping
        return any(fnmatch(key, x) for x in exclude_keys)

    return key_checker


def compare_file_dicts(
    result: dict,
    expected: dict,
    tolerance: Optional[float],
    exclude_keys: Optional[list],
):
    """ """
    test = True
    log = {}
    key_exclude = make_key_checker(exclude_keys)
    keys = set(list(result.keys()) + list(expected.keys()))
    for key in keys:
        temp = True
        if key_exclude(key):
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
                temp, msg = compare_values(res_val, exp_val, tolerance)

        if temp is False:
            test = False
        log.update({key: {"result": temp, "msg": msg}})

    message = "\n".join(
        [f"{k} : {v['msg']}" for k, v in log.items() if v["result"] is False]
    )
    return test, message


def check_extensions_equal(result, expected):
    result_ext = pathlib.Path(result).suffix
    expected_ext = pathlib.Path(expected).suffix
    if result_ext != expected_ext:
        msg = f"The file extensions for result [{result_ext}] and expected [{expected_ext}] do not match."
        raise ValueError(msg)
    return True


def assert_footings_json_files_equal(
    result: str,
    expected: str,
    tolerance: Optional[float] = None,
    exclude_keys: Optional[list] = None,
):
    """Assert whether two footing json files are equal.  This function is
    useful for unit testing models to ensure models stay true over time.

    Parameters
    ----------
    result : str
        The path to the result file.
    expected : str
        The path to the expected file.
    tolerance : float, optional
        The tolerance to test on numeric values.
    exclude_keys : list, optional
        Keys to exclude from testing.

    Returns
    -------
    bool
        True or false on whether files are equal given parameters.

    Raises
    ------
    ValueError
        If the result and expected files share different extension types.
    AssertionError
        If any records between the result and expected files are different.
    """
    check_extensions_equal(result, expected)
    result = load_footings_json_file(result)
    expected = load_footings_json_file(expected)
    test, message = compare_file_dicts(
        result=result, expected=expected, tolerance=tolerance, exclude_keys=exclude_keys,
    )
    if test is False:
        raise AssertionError(f"\n{str(message)}")
    return True


def assert_footings_xlsx_files_equal(
    result: str,
    expected: str,
    tolerance: Optional[float] = None,
    exclude_keys: Optional[list] = None,
):
    """Assert whether two footing xlsx files are equal. This function is
    useful for unit testing models to ensure models stay true over time.

    Parameters
    ----------
    result : str
        The path to the result file.
    expected : str
        The path to the expected file.
    tolerance : float, optional
        The tolerance to test on numeric values.
    exclude_keys : list, optional
        Keys to exclude from testing.

    Returns
    -------
    bool
        True or false on whether files are equal given parameters.

    Raises
    ------
    ValueError
        If the result and expected files share different extension types.
    AssertionError
        If any records between the result and expected files are different.
    """
    check_extensions_equal(result, expected)
    result = load_footings_xlsx_file(result)
    expected = load_footings_xlsx_file(expected)
    test, message = compare_file_dicts(
        result=result, expected=expected, tolerance=tolerance, exclude_keys=exclude_keys,
    )
    if test is False:
        raise AssertionError(f"\n{message}")
    return True


@dispatch_function(key_parameters=("file_ext",))
def _assert_footings_files_equal(file_ext, result, expected, **kwargs):
    """test run_model audit"""
    msg = "No registered function based on passed paramters and no default function."
    raise NotImplementedError(msg)


@_assert_footings_files_equal.register(file_ext=".json")
def _(result: str, expected: str, **kwargs):
    assert_footings_json_files_equal(result, expected, **kwargs)


@_assert_footings_files_equal.register(file_ext=".xlsx")
def _(result: str, expected: str, **kwargs):
    assert_footings_xlsx_files_equal(result, expected, **kwargs)


def assert_footings_files_equal(
    result: str,
    expected: str,
    tolerance: Optional[float] = None,
    exclude_keys: Optional[list] = None,
):
    """A generic function to assert whether two footing files are equal. This function is
    useful for unit testing models to ensure models stay true over time.

    Parameters
    ----------
    result : str
        The path to the result file.
    expected : str
        The path to the expected file.
    tolerance : float, optional
        The tolerance to test on numeric values.
    exclude_keys : list, optional
        Keys to exclude from testing.

    Returns
    -------
    bool
        True or false on whether files are equal given parameters.

    Raises
    ------
    ValueError
        If the result and expected files share different extension types.
    AssertionError
        If any records between the result and expected files are different.

    See Also
    --------
    assert_footings_json_files_equal
    assert_footings_xlsx_files_equal
    """
    file_ext = pathlib.Path(result).suffix
    _assert_footings_files_equal(
        file_ext=file_ext,
        result=result,
        expected=expected,
        tolerance=tolerance,
        exclude_keys=exclude_keys,
    )
