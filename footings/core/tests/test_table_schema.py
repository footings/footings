import pytest
import pandas as pd

from footings.core.table_schema import (
    ColumnSchemaError,
    ColumnSchema,
    TableSchemaError,
    TableSchema,
    table_schema_from_json,
    table_schema_from_yaml,
)


def test_column_schema_dtype():
    schema = ColumnSchema("test", dtype=int)
    test = pd.Series(["1", "2"], dtype="object")
    pytest.raises(ColumnSchemaError, schema.valid, test)


def test_column_schema_nullable():
    schema = ColumnSchema("test", dtype="object", nullable=False)
    test = pd.Series(["1", "2", None, None], dtype="object")
    pytest.raises(ColumnSchemaError, schema.valid, test)


def test_column_schema_allowed():
    schema = ColumnSchema("test", dtype="object", allowed=["1", "2"])
    test = pd.Series(["1", "2", "3"], dtype="object")
    pytest.raises(ColumnSchemaError, schema.valid, test)


def test_column_schema_min_val():
    schema = ColumnSchema("test", dtype=int, min_val=1)
    test = pd.Series([0, 1, 2], dtype=int)
    pytest.raises(ColumnSchemaError, schema.valid, test)


def test_column_schema_max_val():
    schema = ColumnSchema("test", dtype=int, max_val=1)
    test = pd.Series([0, 1, 2], dtype=int)
    pytest.raises(ColumnSchemaError, schema.valid, test)


def test_column_schema_min_len():
    schema = ColumnSchema("test", dtype="object", min_len=1)
    test = pd.Series(["", "00", "01", "02"], dtype="object")
    pytest.raises(ColumnSchemaError, schema.valid, test)


def test_column_schema_max_len():
    schema = ColumnSchema("test", dtype="object", max_len=1)
    test = pd.Series(["", "00", "01", "02"], dtype="object")
    pytest.raises(ColumnSchemaError, schema.valid, test)


def test_column_schema_custom():
    def custom_func(column):
        return [v == "" for i, v in column.iteritems()]

    schema = ColumnSchema("test", dtype="object", custom=custom_func)
    test = pd.Series(["", "00", "01", "02"], dtype="object")
    pytest.raises(ColumnSchemaError, schema.valid, test)


def test_table_schema_column():
    columns = [
        ColumnSchema("a", dtype=int, min_val=1),
        ColumnSchema("b", dtype=int, max_val=3),
    ]
    schema = TableSchema("test", columns)
    test = pd.DataFrame({"a": [0, 1, 2], "b": [2, 3, 4]})
    # schema.valid(test)
    pytest.raises(TableSchemaError, schema.valid, test)


def test_table_schema_min_row():
    columns = [ColumnSchema("a", dtype=int), ColumnSchema("b", dtype=int)]
    schema = TableSchema("test", columns, min_rows=4)
    test = pd.DataFrame({"a": [0, 1, 2], "b": [2, 3, 4]})
    # schema.valid(test)
    pytest.raises(TableSchemaError, schema.valid, test)


def test_table_schema_max_row():
    columns = [ColumnSchema("a", dtype=int), ColumnSchema("b", dtype=int)]
    schema = TableSchema("test", columns, max_rows=2)
    test = pd.DataFrame({"a": [0, 1, 2], "b": [2, 3, 4]})
    # schema.valid(test)
    pytest.raises(TableSchemaError, schema.valid, test)


def test_table_schema_custom():
    def custom_func(table):
        return [1] if table.shape[0] > 2 else [0]

    columns = [ColumnSchema("a", dtype=int), ColumnSchema("b", dtype=int)]
    schema = TableSchema("test", columns, custom=custom_func)
    test = pd.DataFrame({"a": [0, 1, 2], "b": [2, 3, 4]})
    pytest.raises(TableSchemaError, schema.valid, test)


def test_table_schema_enforce_strict():
    columns = [ColumnSchema("a", dtype=int), ColumnSchema("b", dtype=int)]
    schema_enforce = TableSchema("schema_enforce", columns)
    schema_allow = TableSchema("schema_allow", columns, enforce_strict=False)
    test = pd.DataFrame({"a": [0, 1, 2], "b": [2, 3, 4], "c": [1, 2, 3]})

    pytest.raises(TableSchemaError, schema_enforce.valid, test)
    assert schema_allow.valid(test) == True
