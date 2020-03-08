"""Test for table_schemas.py"""

# pylint: disable=function-redefined, missing-function-docstring

import os
import pytest
import pandas as pd
from pandas.testing import assert_series_equal, assert_frame_equal

from footings.schema import (
    ColSchemaError,
    ColSchema,
    TblSchemaError,
    TblSchema,
    table_schema_from_yaml,
    table_schema_from_json,
)


def test_column_schema_dtype():
    schema = ColSchema("test", dtype=int)
    test = pd.Series(["1", "2"], dtype="object")
    pytest.raises(ColSchemaError, schema.valid, test)


def test_column_schema_nullable():
    schema = ColSchema("test", dtype="object", nullable=False)
    test = pd.Series(["1", "2", None, None], dtype="object")
    pytest.raises(ColSchemaError, schema.valid, test)


def test_column_schema_allowed():
    schema = ColSchema("test", dtype="object", allowed=["1", "2"])
    test = pd.Series(["1", "2", "3"], dtype="object")
    pytest.raises(ColSchemaError, schema.valid, test)


def test_column_schema_min_val():
    schema = ColSchema("test", dtype=int, min_val=1)
    test = pd.Series([0, 1, 2], dtype=int)
    pytest.raises(ColSchemaError, schema.valid, test)


def test_column_schema_max_val():
    schema = ColSchema("test", dtype=int, max_val=1)
    test = pd.Series([0, 1, 2], dtype=int)
    pytest.raises(ColSchemaError, schema.valid, test)


def test_column_schema_min_len():
    schema = ColSchema("test", dtype="object", min_len=1)
    test = pd.Series(["", "00", "01", "02"], dtype="object")
    pytest.raises(ColSchemaError, schema.valid, test)


def test_column_schema_max_len():
    schema = ColSchema("test", dtype="object", max_len=1)
    test = pd.Series(["", "00", "01", "02"], dtype="object")
    pytest.raises(ColSchemaError, schema.valid, test)


def test_column_schema_custom():
    def custom_func(column):
        return [v == "" for i, v in column.iteritems()]

    schema = ColSchema("test", dtype="object", custom=custom_func)
    test = pd.Series(["", "00", "01", "02"], dtype="object")
    pytest.raises(ColSchemaError, schema.valid, test)


def test_column_to_pandas_series():
    schema = ColSchema("test", dtype=int)
    assert_series_equal(schema.to_pandas_series(), pd.Series(dtype="int"))


def test_table_schema_column():
    columns = [ColSchema("a", dtype=int, min_val=1), ColSchema("b", dtype=int, max_val=3)]
    schema = TblSchema("test", columns)
    test = pd.DataFrame({"a": [0, 1, 2], "b": [2, 3, 4]})
    pytest.raises(TblSchemaError, schema.valid, test)


def test_table_schema_min_row():
    columns = [ColSchema("a", dtype=int), ColSchema("b", dtype=int)]
    schema = TblSchema("test", columns, min_rows=4)
    test = pd.DataFrame({"a": [0, 1, 2], "b": [2, 3, 4]})
    pytest.raises(TblSchemaError, schema.valid, test)


def test_table_schema_max_row():
    columns = [ColSchema("a", dtype=int), ColSchema("b", dtype=int)]
    schema = TblSchema("test", columns, max_rows=2)
    test = pd.DataFrame({"a": [0, 1, 2], "b": [2, 3, 4]})
    pytest.raises(TblSchemaError, schema.valid, test)


def test_table_schema_custom():
    def custom_func(table):
        return [1] if table.shape[0] > 2 else [0]

    columns = [ColSchema("a", dtype=int), ColSchema("b", dtype=int)]
    schema = TblSchema("test", columns, custom=custom_func)
    test = pd.DataFrame({"a": [0, 1, 2], "b": [2, 3, 4]})
    pytest.raises(TblSchemaError, schema.valid, test)


def test_table_schema_enforce_strict():
    columns = [ColSchema("a", dtype=int), ColSchema("b", dtype=int)]
    schema_enforce = TblSchema("schema_enforce", columns)
    schema_allow = TblSchema("schema_allow", columns, enforce_strict=False)
    test = pd.DataFrame({"a": [0, 1, 2], "b": [2, 3, 4], "c": [1, 2, 3]})

    pytest.raises(TblSchemaError, schema_enforce.valid, test)
    assert schema_allow.valid(test) is True


def test_dataframe_to_pandas_series():
    columns = [ColSchema("a", dtype=int), ColSchema("b", dtype=int)]
    schema = TblSchema("test", columns)
    test = pd.DataFrame({"a": pd.Series(dtype=int), "b": pd.Series(dtype=int)})
    assert_frame_equal(schema.to_pandas_dataframe(), test)


def test_schema_from_yaml():
    file = os.path.join("tests", "data", "test-yaml.yaml")
    schema = table_schema_from_yaml(file)
    print(file)
    print(schema)
    assert 1 == 2


def test_schema_from_json():
    pass
