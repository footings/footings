import os
from datetime import date

import pandas as pd
import pytest
from footings.io.to_xlsx import FootingsXlsxEntry
from footings.testing import (
    compare_values,
    make_key_checker,
    check_extensions_equal,
    assert_footings_files_equal,
    assert_footings_json_files_equal,
    assert_footings_xlsx_files_equal,
)


def test_compare_values():

    # Test string
    assert compare_values("str", "str")[0] is True
    assert compare_values("str", "rts")[0] is False
    assert (
        compare_values("str", "rts")[1]
        == "The result value [str] is not equal to the expected value [rts]."
    )
    long_str = str([x for x in range(0, 200)])
    assert compare_values(long_str, "rts")[0] is False
    assert (
        compare_values(long_str, "rts")[1]
        == "The result and expected strings are not equal and they are to long to display."
    )

    # Test bool
    assert compare_values(True, True)[0] is True
    assert compare_values(True, False)[0] is False
    assert (
        compare_values(True, False)[1]
        == "The result value [True] is not equal to the expected value [False]."
    )

    # Test int
    assert compare_values(0, 0)[0] is True
    assert compare_values(0, 1)[0] is False
    assert (
        compare_values(0, 1)[1]
        == "The result value [0] is not equal to the expected value [1]."
    )

    # Test float
    assert compare_values(0.5, 0.5)[0] is True
    assert compare_values(0.5, 0.6)[0] is False
    assert (
        compare_values(0.5, 0.6)[1]
        == "The result value [0.5] is not equal to the expected value [0.6]."
    )

    # Test tolerance
    assert compare_values(0.5, 0.6, tolerance=0.2)[0] is True
    assert compare_values(0.5, 0.71, tolerance=0.2)[0] is False
    assert (
        compare_values(0.5, 0.71, tolerance=0.2)[1]
        == "The result value [0.5] is not equal to the expected value [0.71] using a tolerance of 0.2."
    )

    # Test list
    assert compare_values([0.5], [0.5])[0] is True
    assert compare_values([0.5], [0.6])[0] is False
    msg = "The result list values are different from the expected list values.\n\n"
    msg += "The result list values are - \n\n[0.5]\n\n"
    msg += "The expected list values are - \n\n[0.6]\n\n"
    assert compare_values([0.5], [0.6])[1] == msg

    # Test pd.Series
    assert compare_values(pd.Series([0.5]), pd.Series([0.5]))[0] is True
    assert compare_values(pd.Series([0.5]), pd.Series([0.6]))[0] is False
    msg = "The result pd.Series is different from the expected pd.Series.\n\n"
    msg += "The result pd.Series is - \n\n[0.5]\n\n"
    msg += "The expected pd.Series is - \n\n[0.6]\n\n"
    assert compare_values(pd.Series([0.5]), pd.Series([0.6]))[1] == msg

    # Test pd.DataFrame
    assert (
        compare_values(pd.DataFrame({"x": [0.5]}), pd.DataFrame({"x": [0.5]}))[0] is True
    )
    assert (
        compare_values(pd.DataFrame({"x": [0.5]}), pd.DataFrame({"x": [0.6]}))[0] is False
    )
    msg = "The result pd.DataFrame is different from the expected pd.DataFrame.\n\n"
    msg += "The result pd.Series for column [x] is different from the expected pd.Series.\n\n"
    msg += "The result pd.Series is - \n\n[0.5]\n\n"
    msg += "The expected pd.Series is - \n\n[0.6]\n\n"
    assert (
        compare_values(pd.DataFrame({"x": [0.5]}), pd.DataFrame({"x": [0.6]}))[1] == msg
    )

    # test date
    assert compare_values(date(2020, 1, 1), date(2020, 1, 1))[0] is True
    assert compare_values(date(2020, 1, 1), date(2020, 1, 2))[0] is False
    assert (
        compare_values(date(2020, 1, 1), date(2020, 1, 2))[1]
        == "The result value [2020-01-01] is not equal to the expected value [2020-01-02]."
    )


def test_key_checker():
    kws = {
        "worksheet": "test_dict",
        "source": None,
        "column_name": None,
        "stable": None,
        "end_point": "KEY",
        "row_start": 2,
        "col_start": 2,
        "row_end": 2,
        "col_end": 2,
        "dtype": "<class 'str'>",
    }

    key_exclude_1 = make_key_checker(["*"])
    assert key_exclude_1("/outer/") is True
    assert key_exclude_1(FootingsXlsxEntry(**kws, mapping="/outer/")) is True

    key_exclude_2 = make_key_checker(["*/inner/"])
    assert key_exclude_2("/outer/inner/") is True
    assert key_exclude_2("/endpoint3/") is False
    assert key_exclude_2(FootingsXlsxEntry(**kws, mapping="/outer/inner/")) is True
    assert key_exclude_2(FootingsXlsxEntry(**kws, mapping="/endpoint3/")) is False

    key_exclude_3 = make_key_checker(["*/endpoint/*"])
    assert key_exclude_3("/outer/inner/") is False
    assert key_exclude_3("/outer/inner/endpoint/") is True
    assert key_exclude_3("/endpoint/") is True
    assert key_exclude_3(FootingsXlsxEntry(**kws, mapping="/outer/inner/")) is False
    assert (
        key_exclude_3(FootingsXlsxEntry(**kws, mapping="/outer/inner/endpoint/")) is True
    )
    assert key_exclude_3(FootingsXlsxEntry(**kws, mapping="/endpoint/")) is True

    key_exclude_4 = make_key_checker(["*/inner/", "*/endpoint/*"])
    assert key_exclude_4("/outer/inner/") is True
    assert key_exclude_4("/outer/inner/endpoint/") is True
    assert key_exclude_4("/endpoint/") is True

    assert key_exclude_4(FootingsXlsxEntry(**kws, mapping="/outer/inner/")) is True
    assert (
        key_exclude_4(FootingsXlsxEntry(**kws, mapping="/outer/inner/endpoint/")) is True
    )
    assert key_exclude_4(FootingsXlsxEntry(**kws, mapping="/endpoint/")) is True


def test_compare_file_dicts():
    pass


def test_check_extensions_equal():
    assert check_extensions_equal("file1.xlsx", "file2.xlsx") is True
    pytest.raises(ValueError, check_extensions_equal, "file1.json", "file2.xlsx")


def test_assert_footings_json_files_equal():
    expected_file = os.path.join("tests", "io", "data", "expected-load-file.json")
    wrong_file = os.path.join("tests", "io", "data", "wrong-load-file.json")

    assert_footings_json_files_equal(expected_file, expected_file)
    assert_footings_files_equal(expected_file, expected_file)
    assert_footings_json_files_equal(
        expected_file, wrong_file, exclude_keys=["*/endpoint*"]
    )

    with pytest.raises(AssertionError):
        assert_footings_json_files_equal(expected_file, wrong_file)
        assert_footings_files_equal(expected_file, wrong_file)
        assert_footings_files_equal(
            expected_file, wrong_file, exclude_keys=["*/endpoint4/"]
        )


def test_assert_footings_xlsx_files_equal():
    expected_file = os.path.join("tests", "io", "data", "expected-load-file.xlsx")
    wrong_file = os.path.join("tests", "io", "data", "wrong-load-file.xlsx")

    assert_footings_xlsx_files_equal(expected_file, expected_file)
    assert_footings_files_equal(expected_file, expected_file)
    assert_footings_xlsx_files_equal(
        expected_file, wrong_file, exclude_keys=["*/endpoint*"]
    )

    with pytest.raises(AssertionError):
        assert_footings_xlsx_files_equal(expected_file, wrong_file)
        assert_footings_files_equal(expected_file, wrong_file)
        assert_footings_files_equal(
            expected_file, wrong_file, exclude_keys=["*/endpoint4/"]
        )
