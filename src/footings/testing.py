import pathlib
from fnmatch import fnmatch
from functools import singledispatch
from typing import Optional

import pandas as pd

from footings.utils import dispatch_function

from .io import load_footings_json_file, load_footings_xlsx_file
from .io.to_xlsx import FootingsXlsxEntry

#########################################################################################
# comparison functions
#########################################################################################

MAX_STR_LEN_TO_SHOW = 100


@singledispatch
def compare_values(result_value, expected_value, tolerance=None):
    """A dispatch function to compare two values."""
    return compare_values(str(result_value), str(expected_value))


@compare_values.register(str)
def _(result_value, expected_value, tolerance=None):
    if type(result_value) != type(expected_value):
        return compare_values(result_value, str(expected_value), tolerance)
    result = result_value == expected_value
    if result:
        msg = ""
    else:
        if (
            len(result_value) <= MAX_STR_LEN_TO_SHOW
            and len(expected_value) <= MAX_STR_LEN_TO_SHOW
        ):
            msg = f"The result value [{result_value}] is not equal to the expected value [{expected_value}]."
        else:
            msg = "The result and expected strings are not equal and they are to long to display."
    return result, msg


@compare_values.register(bool)
@compare_values.register(int)
@compare_values.register(float)
def _(result_value, expected_value, tolerance=None):
    if type(result_value) != type(expected_value):
        return compare_values(str(result_value), str(expected_value))
    if result_value == float("nan") or expected_value == float("nan"):
        return compare_values(str(result_value), str(expected_value))
    if tolerance is None:
        result = result_value == expected_value
    else:
        result = abs(result_value - expected_value) < tolerance
    if result:
        msg = ""
    else:
        msg = f"The result value [{str(result_value)}] is not equal to the expected value [{str(expected_value)}]."
        if tolerance is not None:
            msg = msg[:-1] + f" using a tolerance of {str(tolerance)}."
    return result, msg


@compare_values.register(list)
def _(result_value, expected_value, tolerance=None):
    result = all(
        compare_values(r, e, tolerance)[0] for r, e in zip(result_value, expected_value)
    )
    if result:
        msg = ""
    else:
        msg = "The result list values are different from the expected list values.\n\n"
        msg += f"The result list values are - \n\n{str(result_value)}\n\n"
        msg += f"The expected list values are - \n\n{str(expected_value)}\n\n"
    return result, msg


@compare_values.register(pd.Series)
def _(result_value, expected_value, tolerance=None):
    result = all(
        compare_values(r, e, tolerance)[0] for r, e in zip(result_value, expected_value)
    )
    if result:
        msg = ""
    else:
        if result_value.name is None:
            msg = "The result pd.Series is different from the expected pd.Series.\n\n"
        else:
            msg = f"The result pd.Series for column [{result_value.name}] is different"
            msg += " from the expected pd.Series.\n\n"
        msg += f"The result pd.Series is - \n\n{str(result_value.values)}\n\n"
        msg += f"The expected pd.Series is - \n\n{str(expected_value.values)}\n\n"
    return result, msg


@compare_values.register(pd.DataFrame)
def _(result_value, expected_value, tolerance=None):
    series_compare = [
        compare_values(result_value[r], expected_value[e], tolerance)
        for r, e in zip(result_value.columns, expected_value.columns)
    ]
    result = all(x[0] for x in series_compare)
    if result:
        msg = ""
    else:
        msg = "The result pd.DataFrame is different from the expected pd.DataFrame.\n\n"
        msg += "\n\n".join(x[1] for x in series_compare)
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
            msg = (
                "The result file is missing this key, but it exists in the expected file."
            )
            temp = False

        try:
            exp_val = expected[key]
        except KeyError:
            msg = (
                "The expected file is missing this key, but it exists in the result file."
            )
            temp = False

        if temp:
            if type(res_val) != type(exp_val):
                temp = False
                msg = "The value types are different."
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
