from typing import List, Dict, Tuple, Union, Optional, Any, Callable
from dataclasses import dataclass, field
from dask.utils import Dispatch
import pandas as pd

__all__ = [
    "ColumnSchema",
    "TableSchema",
    "table_schema_from_json",
    "table_schema_from_yaml",
]

#########################################################################################
check_type = Dispatch("check_type")


@check_type.register(pd.Series)
def check_type_pd_series(column, dtype):
    return column.dtypes == dtype


#########################################################################################
check_nullable = Dispatch("check_nullable")


@check_nullable.register(pd.Series)
def check_nullable_pd_series(column):
    return column.isnull().values.sum()


#########################################################################################
check_allowed = Dispatch("check_allowed")


@check_allowed.register(pd.Series)
def check_allowed_pd_series(column, allowed):
    unique = set(column.unique())
    allowed = set(allowed)
    return len(unique - allowed)


#########################################################################################
check_min_val = Dispatch("check_min_val")


@check_min_val.register(pd.Series)
def check_min_val_pd_series(column, min_val):
    return (column < min_val).sum()


#########################################################################################
check_max_val = Dispatch("check_max_val")


@check_max_val.register(pd.Series)
def check_max_val_pd_series(column, max_val):
    return (column > max_val).sum()


#########################################################################################
check_min_len = Dispatch("check_min_len")


@check_min_len.register(pd.Series)
def check_min_len_pd_series(column, min_len):
    return (column.str.len() < min_len).sum()


#########################################################################################
check_max_len = Dispatch("check_max_len")


@check_max_len.register(pd.Series)
def check_max_len_pd_series(column, max_len):
    return (column.str.len() > max_len).sum()


#########################################################################################
check_min_rows = Dispatch("check_min_rows")


@check_min_rows.register(pd.Series)
def check_min_rows_pd_series(column, min_rows):
    return 1 if column.shape[0] < min_rows else 0


@check_min_rows.register(pd.DataFrame)
def check_min_rows_pd_dataframe(table, min_rows):
    return 1 if table.shape[0] < min_rows else 0


#########################################################################################
check_max_rows = Dispatch("check_max_rows")


@check_max_rows.register(pd.Series)
def check_max_rows_pd_series(column, max_rows):
    return 1 if column.shape[0] > max_rows else 0


@check_max_rows.register(pd.DataFrame)
def check_max_rows_pd_dataframe(table, max_rows):
    return 1 if table.shape[0] > max_rows else 0


#########################################################################################
check_custom = Dispatch("check_custom")


@check_custom.register(pd.Series)
def check_custom_pd_series(column, custom):
    return sum(custom(column))


@check_custom.register(pd.DataFrame)
def check_custom_pd_dataframe(dataframe, custom):
    return sum(custom(dataframe))


#########################################################################################
check_enforce_strict = Dispatch("check_enforce_strict")


@check_enforce_strict.register(pd.DataFrame)
def check_enforce_strict_pd_dataframe(table, columns):

    return


#########################################################################################


def _validate_wrapper(x, self, func, attribute=None, test_value=None):
    def validate():
        s = ""
        if attribute is None:
            count = func(x)
            if count > 0:
                s = f"failed {count} times"
            else:
                s = "passed"
        else:
            count = func(x, getattr(self, attribute))
            if count > 0:
                s = f"failed {count} times"
            else:
                s = "passed"
        return s

    if value is not None:
        if test_value is not None:
            if value == test_value:
                s = validate()
            else:
                s = "not validated"
        else:
            s = validate()
    else:
        s = "not validated"

    return s


_WRAPPER_PARAMS_COL = {
    "nullable": {"func": check_nullable, "pass_value": False, "test_value": False},
    "allowed": {"func": check_allowed, "pass_value": True},
    "min_val": {"func": check_min_val, "pass_value": True},
    "max_val": {"func": check_max_val, "pass_value": True},
    "min_len": {"func": check_min_len, "pass_value": True},
    "max_len": {"func": check_max_len, "pass_value": True},
    "custom": {"func": check_custom, "pass_value": True},
}


class ColumnSchemaError(Exception):
    """ """


_WRAPPER_PARAMS_TBL = {
    "min_rows": {"func": check_min_rows, "pass_value": True},
    "max_rows": {"func": check_max_rows, "pass_value": True},
    "custom": {"func": check_custom, "pass_value": True},
    # "enforce_strict": {
    #     "func": check_enforce_strict,
    #     "pass_value": True,
    #     "test_value": True,
    # },
}


class TableSchemaError(Exception):
    """ """


@dataclass(frozen=True)
class ColumnSchema:
    """ """

    name: str
    description: Optional[str] = field(default=None, repr=False)
    dtype: Optional[type] = field(default=Any)
    nullable: Optional[bool] = field(default=None)
    allowed: Optional[List[Any]] = field(default=None)
    min_val: Optional[Any] = field(default=None)
    max_val: Optional[Any] = field(default=None)
    min_len: Optional[int] = field(default=None)
    max_len: Optional[int] = field(default=None)
    custom: Optional[Callable] = field(default=None)

    def _valid(self, column):
        if self.dtype is None:
            return {
                **{"dtype": "not validated"},
                **{
                    k: _validate_wrapper(column, getattr(self, k), **v)
                    for k, v in _WRAPPER_PARAMS_COL.items()
                },
            }
        else:
            if self.dtype != column.dtype:
                return {
                    **{"dtype": "failed and other validations not performed"},
                    **{k: "not validated" for k, v in _WRAPPER_PARAMS_COL.items()},
                }
            else:
                return {
                    **{"dtype": "passed"},
                    **{
                        k: _validate_wrapper(column, getattr(self, k), **v)
                        for k, v in _WRAPPER_PARAMS_COL.items()
                    },
                }

    def valid(self, column: str, return_only_errors: bool = True):
        d = self._valid(column)
        failed = any(["failed" in v for k, v in d.items()])
        if failed:
            if return_only_errors:
                raise ColumnSchemaError({k: v for k, v in d.items() if "failed" in v})
            else:
                raise ColumnSchemaError(d)
        else:
            return True

    def to_pandas_series(self):
        pass


@dataclass(frozen=True)
class TableSchema:
    """ """

    name: str
    columns: List[ColumnSchema]
    description: str = field(default=None)
    min_rows: Optional[int] = field(default=None)
    max_rows: Optional[int] = field(default=None)
    custom: Optional[Callable] = field(default=None)
    enforce_strict: bool = True

    def valid(self, table, return_only_errors: bool = True):
        # test columns
        column_validations = {c.name: c._valid(table[c.name]) for c in self.columns}
        column_errors = {
            k: {k2: v2}
            for k, v in column_validations.items()
            for k2, v2 in v.items()
            if "failed" in v2
        }
        column_failed = False if len(column_errors) == 0 else True

        # test table
        table_validations = {
            k: _validate_wrapper(table, getattr(self, k), **v)
            for k, v in _WRAPPER_PARAMS_TBL.items()
        }
        table_errors = {k: v for k, v in table_validations.items() if "failed" in v}
        table_failed = False if len(table_errors) == 0 else True

        if column_failed or table_failed:
            if return_only_errors:
                raise TableSchemaError({**column_errors, **table_errors})
            else:
                raise TableSchemaError(d)
        else:
            return True

    def to_pandas_dataframe(self):
        pass


def table_schema_from_yaml(file):
    """ """
    pass


def table_schema_from_json(file):
    """ """
    pass
