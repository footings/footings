import os
from datetime import date

import pandas as pd
import pytest

from footings.io.to_xlsx import FootingsXlsxEntry
from footings.testing import (
    assert_footings_files_equal,
    assert_footings_json_files_equal,
    assert_footings_xlsx_files_equal,
    check_extensions_equal,
    compare_file_dicts,
    compare_values,
    make_key_checker,
)


class Test_compare_values:
    def test_str(self):
        # short str
        assert compare_values("str", "str") == (True, "")
        msg = "The test_value [str] is not equal to the expected_value [rts]."
        assert compare_values("str", "rts") == (False, msg)

        # long str
        long_str = str([x for x in range(0, 200)])
        msg = "The test_value and expected_value strings are not equal and they are to long to display."
        assert compare_values(long_str, "rts") == (False, msg)

    def test_bool(self):
        assert compare_values(True, True) == (True, "")
        msg = "The test_value [True] is not equal to the expected_value [False]."
        assert compare_values(True, False) == (False, msg)

    def test_int(self):
        assert compare_values(0, 0) == (True, "")
        msg = "The test_value [0] is not equal to the expected_value [1]."
        assert compare_values(0, 1) == (False, msg)

    def test_float(self):
        assert compare_values(0.5, 0.5) == (True, "")
        msg = "The test_value [0.5] is not equal to the expected_value [0.6]."
        assert compare_values(0.5, 0.6) == (False, msg)

        # with tolerance
        assert compare_values(0.5, 0.6, tolerance=0.2) == (True, "")
        msg = "The test_value [0.5] is not equal to the expected_value [0.71] using a tolerance of 0.2."
        assert compare_values(0.5, 0.71, tolerance=0.2) == (False, msg)

    def test_list(self):
        # pass
        assert compare_values([0.5], [0.5]) == (True, "")

        # fail expected type
        msg = "The expected_value is not type list."
        assert compare_values([0.5], (0.5,)) == (False, msg)

        # fail different len
        msg = "The list values have different lengths."
        assert compare_values([0.5], [0.5, 0.6]) == (False, msg)

        # fail different values
        msg = "The test_value list values are different from the expected_value list values (equal = 0.00%).\n\n"
        msg += "The test_value list values are - \n\n[0.5]\n\n"
        msg += "The expected_value list values are - \n\n[0.6]\n\n"
        assert compare_values([0.5], [0.6]) == (False, msg)

    def test_dict(self):
        # pass
        assert compare_values({"x": 1}, {"x": 1}) == (True, "")

        # fail expected type
        msg = "The expected_value is not type dict."
        assert compare_values({"x": 1}, [2]) == (False, msg)

        # fail different keys
        msg = "The test_value and expected_value have different keys.\n\n"
        msg += "The test_value keys are - \n\n['y']\n\n"
        msg += "The expected_value keys are - \n\n['x']\n\n"
        assert compare_values({"y": 1}, {"x": 1}) == (False, msg)

        # fail different values
        msg = "x : The test_value [1] is not equal to the expected_value [2]."
        assert compare_values({"x": 1}, {"x": 2}) == (False, msg)

    def test_pandas_series(self):
        # pass
        assert compare_values(pd.Series([0.5]), pd.Series([0.5])) == (True, "")

        # fail expected type
        msg = "The expected_value is not type pd.Series."
        assert compare_values(pd.Series([0.5], name="X"), [0.5]) == (False, msg,)

        # fail names
        msg = "The test_value name [X] is different then the expected_value name [Y]."
        assert compare_values(pd.Series([0.5], name="X"), pd.Series([0.5], name="Y")) == (
            False,
            msg,
        )

        # fail dtypes
        msg = "The test_value dtype [float16] is different then the expected_value dtype [float32]."
        assert compare_values(
            pd.Series([0.5], dtype="float16"), pd.Series([0.5], dtype="float32")
        ) == (False, msg)

        # fail values
        msg = "The test_value pd.Series values are different from the expected_value "
        msg += "pd.Series values (equal = 50.00%).\n\n"
        msg += "The test_value pd.Series values are - \n\n[0.5, 0.5]\n\n"
        msg += "The expected_value pd.Series values are - \n\n[0.5, 0.6]\n\n"
        assert compare_values(pd.Series([0.5, 0.5]), pd.Series([0.5, 0.6])) == (
            False,
            msg,
        )

    def test_pandas_dataframe(self):
        # pass
        assert compare_values(pd.DataFrame({"x": [0.5]}), pd.DataFrame({"x": [0.5]})) == (
            True,
            "",
        )

        # fail expected type
        assert compare_values(pd.DataFrame({"x": [0.5]}), pd.Series([0.5])) == (
            False,
            "The expected_value is not type pd.DataFrame.",
        )

        # fail different columns
        msg = "The test_value and expected_value DataFrames have different columns.\n\n"
        msg += "The test_value columns are - \n\n['x']\n\n"
        msg += "The expected_value columns are - \n\n['y']\n\n"

        assert compare_values(pd.DataFrame({"x": [0.5]}), pd.DataFrame({"y": [0.5]})) == (
            False,
            msg,
        )

        # fail values
        msg = "The test_value and expected_value pd.DataFrames are different.\n\n"
        msg += "For column, x - \n\n"
        msg += "\tThe test_value pd.Series values are - \n\n\t[0.5]\n\n"
        msg += "\tThe expected_value pd.Series values are - \n\n\t[0.6]\n\n\t"
        assert compare_values(pd.DataFrame({"x": [0.5]}), pd.DataFrame({"x": [0.6]})) == (
            False,
            msg,
        )

    def test_date(self):
        assert compare_values(date(2020, 1, 1), date(2020, 1, 1)) == (True, "")
        msg = (
            "The test_value [2020-01-01] is not equal to the expected_value [2020-01-02]."
        )
        assert compare_values(date(2020, 1, 1), date(2020, 1, 2)) == (False, msg)


def test_key_checker():
    kws = {
        "worksheet": "test_dict",
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
    # pass
    test_dict = {"x": 1, "y": 2}
    expected_dict = {"x": 1, "y": 2}
    assert compare_file_dicts(test_dict, expected_dict) == (True, "")

    # fail
    test_dict = {"x": 1, "y": 2, "w": "1"}
    expected_dict = {"x": 1, "z": 3, "w": 1}
    msgs = (
        "w : The value types are different.",
        "y : The expected file is missing this key, but it exists in the test file.",
        "z : The test file is missing this key, but it exists in the expected file.",
    )
    assert compare_file_dicts(test_dict, expected_dict) == (False, "\n".join(msgs))


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
