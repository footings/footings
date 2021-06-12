import pathlib
from fnmatch import fnmatch
from functools import singledispatch
from typing import Any, Optional, Union

import pandas as pd

from footings.utils import dispatch_function

from .io import load_footings_json_file, load_footings_xlsx_file
from .io.to_xlsx import FootingsXlsxEntry

#########################################################################################
# comparison functions
#########################################################################################

MAX_STR_LEN_TO_SHOW = 100


@singledispatch
def compare_values(
    test_value: Any, expected_value: Any, tolerance: Union[float, None] = None
):
    """A dispatch function to compare value against an expecte value.

    :param Any test_value: The value to test.
    :param Any expected_value: The value expected.
    :param Union[float,None] tolerance: The tolerance (absolute) level to use when testing.

    :return: A tuple with first position being bool and the result of the comparison and the
        second is the comaprison message (if True the message is an empty string).
    :rtype: Tuple[bool, str]
    """
    return compare_values(str(test_value), str(expected_value))


@compare_values.register(str)
def _(test_value, expected_value, tolerance=None):
    if type(test_value) != type(expected_value):
        return compare_values(test_value, str(expected_value), tolerance)
    result = test_value == expected_value
    if result:
        msg = ""
    else:
        if (
            len(test_value) <= MAX_STR_LEN_TO_SHOW
            and len(expected_value) <= MAX_STR_LEN_TO_SHOW
        ):
            msg = f"The test_value [{test_value}] is not equal to the expected_value [{expected_value}]."
        else:
            msg = "The test_value and expected_value strings are not equal and they are to long to display."
    return result, msg


@compare_values.register(bool)
@compare_values.register(int)
@compare_values.register(float)
def _(test_value, expected_value, tolerance=None):
    if type(test_value) != type(expected_value):
        return compare_values(str(test_value), str(expected_value))
    if test_value == float("nan") or expected_value == float("nan"):
        return compare_values(str(test_value), str(expected_value))
    if tolerance is None:
        result = test_value == expected_value
    else:
        result = abs(test_value - expected_value) < tolerance
    if result:
        msg = ""
    else:
        msg = f"The test_value [{str(test_value)}] is not equal to the expected_value [{str(expected_value)}]."
        if tolerance is not None:
            msg = msg[:-1] + f" using a tolerance of {str(tolerance)}."
    return result, msg


@compare_values.register(list)
def _(test_value, expected_value, tolerance=None):
    if isinstance(expected_value, list) is False:
        return False, "The expected_value is not type list."
    if len(test_value) != len(expected_value):
        result = False
        msg = "The list values have different lengths."
        return result, msg
    results = [
        compare_values(r, e, tolerance)[0] for r, e in zip(test_value, expected_value)
    ]
    result = all(results)
    if len(results) > 0:
        correct = "{:.2%}".format(sum(results) / len(results))
    else:
        correct = "0 len list"
    if result:
        msg = ""
    else:
        msg = f"The test_value list values are different from the expected_value list values (equal = {correct}).\n\n"
        msg += f"The test_value list values are - \n\n{str(test_value)}\n\n"
        msg += f"The expected_value list values are - \n\n{str(expected_value)}\n\n"
    return result, msg


@compare_values.register(dict)
def _(test_value, expected_value, tolerance=None):
    if isinstance(expected_value, dict) is False:
        return False, "The expected_value is not type dict."
    if set(test_value.keys()) != set(expected_value.keys()):
        msg = "The test_value and expected_value have different keys.\n\n"
        msg += f"The test_value keys are - \n\n{str(list(test_value.keys()))}\n\n"
        msg += f"The expected_value keys are - \n\n{str(list(expected_value.keys()))}\n\n"
        return False, msg
    result = True
    msgs = []
    for k in test_value.keys():
        temp_r, temp_m = compare_values(test_value[k], expected_value[k], tolerance)
        if temp_r is False:
            result = False
            msgs.append(f"{k} : {temp_m}")
    return result, "\n".join(msgs)


@compare_values.register(pd.Series)
def _(test_value, expected_value, tolerance=None):
    if isinstance(expected_value, pd.Series) is False:
        return False, "The expected_value is not type pd.Series."
    if test_value.name != expected_value.name:
        msg = f"The test_value name [{test_value.name}] is different then the "
        msg += f"expected_value name [{expected_value.name}]."
        return (False, msg)
    if test_value.dtype != expected_value.dtype:
        msg = f"The test_value dtype [{test_value.dtype}] is different then the "
        msg += f"expected_value dtype [{expected_value.dtype}]."
        return (False, msg)
    result, msg = compare_values(
        test_value.to_list(), expected_value.to_list(), tolerance
    )
    if result is False:
        msg = msg.replace("list", "pd.Series")
    return result, msg


@compare_values.register(pd.DataFrame)
def _(test_value, expected_value, tolerance=None):
    def _format_str(x):
        name = x[0]
        results = x[1][1]
        inner = "\n\n\t".join(results.split("\n\n")[1:])
        return f"For column, {name} - \n\n\t{inner}"

    if isinstance(expected_value, pd.DataFrame) is False:
        return False, "The expected_value is not type pd.DataFrame."
    if set(test_value.columns) != set(expected_value.columns):
        msg = "The test_value and expected_value DataFrames have different columns.\n\n"
        msg += f"The test_value columns are - \n\n{str(list(test_value.columns))}\n\n"
        msg += f"The expected_value columns are - \n\n{str(list(expected_value.columns))}\n\n"
        return False, msg
    series_compare = [
        (
            col,
            compare_values(test_value[col], expected_value[col], tolerance),
        )
        for col in test_value.columns
    ]
    result = all(x[1][0] for x in series_compare)
    if result:
        msg = ""
    else:
        msg = "The test_value and expected_value pd.DataFrames are different.\n\n"
        msg += "\n".join(_format_str(x) for x in series_compare)
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
    test: dict,
    expected: dict,
    tolerance: Union[float, None] = None,
    exclude_keys: Union[list, None] = None,
):
    """Compare file dicts generated from footings framework."""
    result = True
    log = {}
    key_exclude = make_key_checker(exclude_keys)
    keys = set(list(test.keys()) + list(expected.keys()))
    for key in keys:
        temp = True
        if key_exclude(key):
            continue
        try:
            res_val = test[key]
        except KeyError:
            msg = "The test file is missing this key, but it exists in the expected file."
            temp = False

        try:
            exp_val = expected[key]
        except KeyError:
            msg = "The expected file is missing this key, but it exists in the test file."
            temp = False

        if temp:
            if type(res_val) != type(exp_val):
                temp = False
                msg = "The value types are different."
            else:
                temp, msg = compare_values(res_val, exp_val, tolerance)

        if temp is False:
            result = False
        log.update({key: {"result": temp, "msg": msg}})

    skeys = sorted(log.keys())
    message = "\n".join(
        [f"{k} : {log.get(k)['msg']}" for k in skeys if log.get(k)["result"] is False]
    )
    return result, message


def check_extensions_equal(test, expected):
    test_ext = pathlib.Path(test).suffix
    expected_ext = pathlib.Path(expected).suffix
    if test_ext != expected_ext:
        msg = f"The file extensions for test [{test_ext}] and expected [{expected_ext}] do not match."
        raise ValueError(msg)
    return True


def assert_footings_json_files_equal(
    test: str,
    expected: str,
    tolerance: Union[float, None] = None,
    exclude_keys: Union[list, None] = None,
):
    """Assert whether two footing json files are equal.  This function is
    useful for unit testing models to ensure models stay true over time.

    :param str test: The path to the test file.
    :param str expected: The path to the expected file.
    :param Union[float,None] tolerance: The tolerance to test on numeric values.
    :param Union[list,None] exclude_keys: Keys to exclude from testing.

    :return: True or false on whether files are equal given parameters.
    :rtype: bool

    :raises ValueError: If the test and expected files share different extension types.
    :raises AssertionError: If any records between the test and expected files are different.
    """
    check_extensions_equal(test, expected)
    test = load_footings_json_file(test)
    expected = load_footings_json_file(expected)
    result, message = compare_file_dicts(
        test=test,
        expected=expected,
        tolerance=tolerance,
        exclude_keys=exclude_keys,
    )
    if result is False:
        raise AssertionError(f"\n{str(message)}")
    return True


def assert_footings_xlsx_files_equal(
    test: str,
    expected: str,
    tolerance: Union[float, None] = None,
    exclude_keys: Union[list, None] = None,
):
    """Assert whether two footing xlsx files are equal.  This function is
    useful for unit testing models to ensure models stay true over time.

    :param str test: The path to the test file.
    :param str expected: The path to the expected file.
    :param Union[float,None] tolerance: The tolerance to test on numeric values.
    :param Union[list,None] exclude_keys: Keys to exclude from testing.

    :return: True or false on whether files are equal given parameters.
    :rtype: bool

    :raises ValueError: If the test and expected files share different extension types.
    :raises AssertionError: If any records between the test and expected files are different.
    """
    check_extensions_equal(test, expected)
    test = load_footings_xlsx_file(test)
    expected = load_footings_xlsx_file(expected)
    result, message = compare_file_dicts(
        test=test,
        expected=expected,
        tolerance=tolerance,
        exclude_keys=exclude_keys,
    )
    if result is False:
        raise AssertionError(f"\n{message}")
    return True


@dispatch_function(key_parameters=("file_ext",))
def _assert_footings_files_equal(file_ext, test, expected, **kwargs):
    """test run_model audit"""
    msg = "No registered function based on passed paramters and no default function."
    raise NotImplementedError(msg)


@_assert_footings_files_equal.register(file_ext=".json")
def _(test: str, expected: str, **kwargs):
    assert_footings_json_files_equal(test, expected, **kwargs)


@_assert_footings_files_equal.register(file_ext=".xlsx")
def _(test: str, expected: str, **kwargs):
    assert_footings_xlsx_files_equal(test, expected, **kwargs)


def assert_footings_files_equal(
    test: str,
    expected: str,
    tolerance: Union[float, None] = None,
    exclude_keys: Union[list, None] = None,
):
    """A generic function to assert whether two footing files are equal. This function is
    useful for unit testing models to ensure models stay true over time.

    :param str test: The path to the test file.
    :param str expected: The path to the expected file.
    :param Union[float,None] tolerance: The tolerance to test on numeric values.
    :param Union[list,None] exclude_keys: Keys to exclude from testing.

    :return: True or false on whether files are equal given parameters.
    :rtype: bool

    :raises ValueError: If the test and expected files share different extension types.
    :raises AssertionError: If any records between the test and expected files are different.

    .. seealso::

        :obj:`footings.testing.assert_footings_json_files_equal`
        :obj:`footings.testing.assert_footings_xlsx_files_equal`
    """
    file_ext = pathlib.Path(test).suffix
    _assert_footings_files_equal(
        file_ext=file_ext,
        test=test,
        expected=expected,
        tolerance=tolerance,
        exclude_keys=exclude_keys,
    )
