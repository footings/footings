import os
import pytest
from footings.to_xlsx import FootingsXlsxEntry
from footings.test_tools.compare_files import (
    make_key_checker,
    check_extensions_equal,
    assert_footings_files_equal,
    assert_footings_json_files_equal,
    assert_footings_xlsx_files_equal,
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
    expected_file = os.path.join("tests", "test_tools", "data", "expected-load-file.json")
    wrong_file = os.path.join("tests", "test_tools", "data", "wrong-load-file.json")

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
    expected_file = os.path.join("tests", "test_tools", "data", "expected-load-file.xlsx")
    wrong_file = os.path.join("tests", "test_tools", "data", "wrong-load-file.xlsx")

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
