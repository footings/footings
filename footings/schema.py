"""table_schema.py"""

import os
import json
from typing import List, Optional, Any, Callable, Union
from functools import singledispatch

import yaml
from attr import attrs, attrib
import pandas as pd

__all__ = ["ColSchema", "TblSchema", "table_schema_from_json", "table_schema_from_yaml"]

#########################################################################################
@singledispatch
def check_type(column, dtype):
    """Check type of column"""
    raise NotImplementedError(f"check not implemented for {type(column)}")


@check_type.register(pd.Series)
def _(column, dtype):
    return column.dtypes == dtype


#########################################################################################
@singledispatch
def check_nullable(column):
    """Check for null values in a column"""
    raise NotImplementedError(f"check not implemented for {type(column)}")


@check_nullable.register(pd.Series)
def _(column):
    return column.isnull().values.sum()


#########################################################################################
@singledispatch
def check_allowed(column, allowed):
    """Check allowed values in column"""
    raise NotImplementedError(f"check not implemented for {type(column)}")


@check_allowed.register(pd.Series)
def _(column, allowed):
    unique = set(column.unique())
    allowed = set(allowed)
    return len(unique - allowed)


#########################################################################################
@singledispatch
def check_min_val(column, min_val):
    """Check min values in a column"""
    raise NotImplementedError(f"check not implemented for {type(column)}")


@check_min_val.register(pd.Series)
def _(column, min_val):
    return (column < min_val).sum()


#########################################################################################
@singledispatch
def check_max_val(column, max_val):
    """Check max values in a column"""
    raise NotImplementedError(f"check not implemented for {type(column)}")


@check_max_val.register(pd.Series)
def _(column, max_val):
    return (column > max_val).sum()


#########################################################################################
@singledispatch
def check_min_len(column, min_len):
    """Check min length of values in a column"""
    raise NotImplementedError(f"check not implemented for {type(column)}")


@check_min_len.register(pd.Series)
def _(column, min_len):
    return (column.str.len() < min_len).sum()


#########################################################################################
@singledispatch
def check_max_len(column, max_len):
    """Check max length of values in a column"""
    raise NotImplementedError(f"check not implemented for {type(column)}")


@check_max_len.register(pd.Series)
def _(column, max_len):
    return (column.str.len() > max_len).sum()


#########################################################################################
@singledispatch
def check_min_rows(table, min_rows):
    """Check min number of rows in a table"""
    raise NotImplementedError(f"check not implemented for {type(table)}")


@check_min_rows.register(pd.Series)
def _(table, min_rows):
    return 1 if table.shape[0] < min_rows else 0


@check_min_rows.register(pd.DataFrame)
def _(table, min_rows):
    return 1 if table.shape[0] < min_rows else 0


#########################################################################################
@singledispatch
def check_max_rows(table, max_rows):
    """Check max number of rows in a table"""
    raise NotImplementedError(f"check not implemented for {type(table)}")


@check_max_rows.register(pd.Series)
def _(table, max_rows):
    return 1 if table.shape[0] > max_rows else 0


@check_max_rows.register(pd.DataFrame)
def _(table, max_rows):
    return 1 if table.shape[0] > max_rows else 0


#########################################################################################
@singledispatch
def check_custom(table, custom):
    """Check custom function against table"""
    raise NotImplementedError(f"check not implemented for {type(table)}")


@check_custom.register(pd.Series)
def _(table, custom):
    return sum(custom(table))


@check_custom.register(pd.DataFrame)
def _(table, custom):
    return sum(custom(table))


#########################################################################################
@singledispatch
def check_enforce_strict(table, columns):
    """Check enforce strict columns in table"""
    raise NotImplementedError(f"check not implemented for {type(table)}")


@check_enforce_strict.register(pd.DataFrame)
def _(table, columns):
    expected = {c.name for c in columns}
    received = set(table.columns)
    return 1 if len(received - expected) > 0 else 0


#########################################################################################


def _validate_wrapper(x, obj, key, func, attributes=None, test_value=None):
    """ """

    def validate():
        s = ""
        if attributes is None:
            count = func(x)
            if count > 0:
                s = f"failed {count} times"
            else:
                s = "passed"
        else:
            count = func(x, *(getattr(obj, a) for a in attributes))
            if count > 0:
                s = f"failed {count} times"
            else:
                s = "passed"
        return s

    if getattr(obj, key) is not None:
        if test_value is not None:
            if getattr(obj, key) == test_value:
                s = validate()
            else:
                s = "not validated"
        else:
            s = validate()
    else:
        s = "not validated"

    return s


_WRAPPER_PARAMS_COL = {
    "nullable": {"func": check_nullable, "test_value": False},
    "allowed": {"func": check_allowed, "attributes": ["allowed"]},
    "min_val": {"func": check_min_val, "attributes": ["min_val"]},
    "max_val": {"func": check_max_val, "attributes": ["max_val"]},
    "min_len": {"func": check_min_len, "attributes": ["min_len"]},
    "max_len": {"func": check_max_len, "attributes": ["max_len"]},
    "custom": {"func": check_custom, "attributes": ["custom"]},
}


class ColSchemaError(Exception):
    """Column schema error"""


_WRAPPER_PARAMS_TBL = {
    "min_rows": {"func": check_min_rows, "attributes": ["min_rows"]},
    "max_rows": {"func": check_max_rows, "attributes": ["max_rows"]},
    "custom": {"func": check_custom, "attributes": ["custom"]},
    "enforce_strict": {
        "func": check_enforce_strict,
        "attributes": ["columns"],
        "test_value": True,
    },
}


class TblSchemaError(Exception):
    """Table schema error"""


@attrs(slots=True, frozen=True)
class ColSchema:
    """ColSchema

    Parameters
    ----------
    name: str
        Column name
    dtype: type or str
        Column type
    description: str
        Column description
    nullable: bool
        Can the column be nullable
    allowed: bool
        Allowed values for the column
    min_val: Any
        The minimum value for the column
    max_val: Any
        The maximum value for the column
    min_len: int
        The minimum length for a value in the column
    max_len: int
        The maximum length for a value in the column
    custom: callable
        A custom function to validate a column

    """

    # pylint: disable=too-many-instance-attributes
    name: str = attrib()
    dtype: Union[type, str] = attrib()
    description: Optional[str] = attrib(default=None, repr=False)
    nullable: Optional[bool] = attrib(default=None)
    allowed: Optional[List[Any]] = attrib(default=None)
    min_val: Optional[Any] = attrib(default=None)
    max_val: Optional[Any] = attrib(default=None)
    min_len: Optional[int] = attrib(default=None)
    max_len: Optional[int] = attrib(default=None)
    custom: Optional[Callable] = attrib(default=None)

    def _valid(self, column):
        if self.dtype != column.dtype:
            return {
                **{"dtype": "failed and other validations not performed"},
                **{k: "not validated" for k, v in _WRAPPER_PARAMS_COL.items()},
            }

        return {
            **{"dtype": "passed"},
            **{
                k: _validate_wrapper(column, obj=self, key=k, **v)
                for k, v in _WRAPPER_PARAMS_COL.items()
            },
        }

    def valid(self, column: str, return_only_errors: bool = True):
        """valid"""
        dict_ = self._valid(column)
        failed = any(["failed" in v for k, v in dict_.items()])
        if failed:
            if return_only_errors:
                raise ColSchemaError({k: v for k, v in dict_.items() if "failed" in v})
            raise ColSchemaError(dict_)
        return True

    def to_pandas_series(self):
        """Create empty pandas series"""
        return pd.Series(dtype=self.dtype)


@attrs(slots=True, frozen=True, repr=False)
class TblSchema:
    """TblSchema

    Parameters
    ----------
    name: str
        Column name
    columns:
        List of ColSchemas
    dtype: type or str
        Table type
    description: str
        Column description
    min_rows: int
        The minimum number of rows for the table
    max_rows: int
        The maximum number of rows for the table
    custom: callable
        A custom function to validate a column
    enforce_strict: bool
        True every column has to be specified; False columns not specified are allowed
    """

    name: str = attrib()
    columns: List[ColSchema] = attrib()
    dtype: type = attrib(default=pd.DataFrame)
    description: str = attrib(default=None)
    min_rows: Optional[int] = attrib(default=None)
    max_rows: Optional[int] = attrib(default=None)
    custom: Optional[Callable] = attrib(default=None)
    enforce_strict: bool = attrib(default=True)

    def valid(self, table, return_only_errors: bool = True):
        """valid"""
        # test columns
        column_validations = {
            c.name: c._valid(table[c.name])  # pylint: disable=protected-access
            for c in self.columns
        }
        column_errors = {
            k: {k2: v2}
            for k, v in column_validations.items()
            for k2, v2 in v.items()
            if "failed" in v2
        }
        column_failed = not len(column_errors) == 0

        # test table
        table_validations = {
            k: _validate_wrapper(table, obj=self, key=k, **v)
            for k, v in _WRAPPER_PARAMS_TBL.items()
        }

        table_errors = {k: v for k, v in table_validations.items() if "failed" in v}

        table_failed = not len(table_errors) == 0

        if column_failed or table_failed:
            if return_only_errors:
                raise TblSchemaError({**column_errors, **table_errors})
            raise TblSchemaError({**column_validations, **table_validations})

        return True

    def create_validator(self):
        """Create validator for table"""

        def validator(inst, attribute, value):
            return self.valid(value, True)

        return validator

    def to_pandas_dataframe(self):
        """Create empty pandas dataframe"""
        return pd.concat(
            {column.name: column.to_pandas_series() for column in self.columns}, axis=1
        )

    def __repr__(self):
        return f"TblSchema({self.name})"


class TableSchemaFromYamlError(Exception):
    """Error raised trying to read in table schema from yaml."""


def _create_table_schema(**kwargs):
    if not isinstance(kwargs, dict):
        raise TableSchemaFromYamlError("The loaded file is not a dict. See examples.")
    if "columns" not in kwargs:
        raise TableSchemaFromYamlError(
            "The loaded file does not have columns specified. See examples."
        )
    if not isinstance(kwargs["columns"], list):
        raise TableSchemaFromYamlError("The loaded columns are not a list. See examples.")
    kwargs["columns"] = [ColSchema(**col_specs) for col_specs in kwargs["columns"]]
    return TblSchema(**kwargs)


def table_schema_from_yaml(file):
    """Create table schema from yaml file"""
    if not os.path.exists(file):
        raise FileExistsError(f"The file [{file}] does not exist.")
    with open(file, "r") as stream:
        kwargs = yaml.safe_load(stream)
    return _create_table_schema(**kwargs)


def table_schema_from_json(file):
    """Create table schema from json file"""
    if not os.path.exists(file):
        raise FileExistsError(f"The file [{file}] does not exist.")
    with open(file, "r") as stream:
        kwargs = json.loads(stream)
    return _create_table_schema(**kwargs)
