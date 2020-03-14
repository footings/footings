"""functions.py"""

from functools import partial, singledispatch

from attr import attrs, attrib
from attr.validators import optional, is_callable, instance_of
import pandas as pd
from pandas.io.json import build_table_schema

from .schema import TblSchema, ColSchema
from .utils import Dispatcher
from .errors import (
    FootingsNestedTaskError,
    FootingsTaskCallError,
    FootingsTaskCreationError,
    FootingsTaskEnterError,
    FootingsTaskExitError,
)


#########################################################################################
# NestedTask
#########################################################################################


@attrs(slots=True, frozen=True)
class NestedTask:
    """Post hook"""

    function = attrib(validator=is_callable())
    post_function = attrib(validator=is_callable())

    def __call__(self, *args, **kwargs):
        return self.post_function(self.function(*args, **kwargs))


def make_nested(function=None, post_function=None):
    """Function or decorator to create a nested function."""
    if function is None:
        return partial(NestedTask, post_function=post_function)
    return NestedTask(function=function, post_function=post_function)


@singledispatch
def remove_columns(tbl, columns):
    """remove columns"""
    raise NotImplementedError(f"remove_columns has not been defined for [{type(tbl)}].")


@remove_columns.register(pd.DataFrame)
def _(tbl, columns):
    return tbl.drop(columns, axis=1)


def make_nested_remove_columns(function=None, columns=None):
    """Function or decorator to create a nested function."""
    if columns is None:
        post_function = None
    else:

        def post_function(tbl):
            return remove_columns(tbl, columns)

    if function is None:
        return partial(NestedTask, post_function=post_function)
    return NestedTask(function=function, post_function=post_function)


#########################################################################################
# FootingsTask
#########################################################################################


@singledispatch
def capture_state(obj):
    """capture_state"""
    raise NotImplementedError(f"capture_state has not been defined for [{type(obj)}].")


@capture_state.register(pd.DataFrame)
def _(tbl):
    int_schema = build_table_schema(tbl)["fields"]
    return [(x["name"], x["type"]) for x in int_schema]


@attrs(slots=True, frozen=True, repr=False)
class FootingsTask:
    """FootingsTask"""

    name = attrib(validator=instance_of(str))
    function = attrib(validator=is_callable())
    test_enter = attrib(default=None, validator=optional(is_callable()))
    test_exit = attrib(default=None, validator=optional(is_callable()))

    def __call__(self, **kwargs):
        obj = kwargs.get(self.name, None)
        if obj is None:
            msg = f"The object [{self.name}] was not detected in kwargs."
            raise FootingsTaskCallError(msg)
        self.test_enter(obj)
        state_enter = capture_state(obj)
        obj = self.function(**kwargs)
        self.test_exit(obj, state_enter)
        return obj


create_task = Dispatcher("create_task", parameters=("method",))


def _validate_columns_present(tbl_cols, test_cols, src, error):
    diff = test_cols.difference(tbl_cols)
    if len(diff) > 0:
        msg = f"{src}"
        raise error(msg)


def _validate_columns_not_present(tbl_cols, test_cols, src, error):
    shared = tbl_cols.intersection(test_cols)
    if len(shared) > 0:
        msg = f"{shared}"
        raise error(msg)


def _create_test_enter_func(
    required_columns, removed_columns, added_columns, returned_columns
):
    error = FootingsTaskEnterError

    def test_enter_function(tbl):
        tbl_cols = set(tbl.columns)
        if required_columns is not None:
            _validate_columns_present(
                tbl_cols, set(required_columns), "required_columns", error
            )
        if removed_columns is not None:
            _validate_columns_present(
                tbl_cols, set(removed_columns), "removed_columns", error
            )
        if added_columns is not None:
            added = set(col.name for col in added_columns)
            _validate_columns_not_present(tbl_cols, added, "added_columns", error)
        else:
            added = set()
        if returned_columns is not None:
            returned = set(returned_columns).difference(added)
            _validate_columns_present(tbl_cols, returned, "returned_columns", error)

    return test_enter_function


def _create_test_exit_func(
    exp_removed_columns, exp_added_columns, exp_returned_columns, exp_returned_schema
):
    error = FootingsTaskExitError

    def test_exit_function(tbl, tbl_schema_enter):
        act_tbl_schema = capture_state(tbl)
        act_returned_cols = set(col[0] for col in act_tbl_schema)
        act_returned_cols_prior = set(col[0] for col in tbl_schema_enter)

        if exp_returned_schema is not None:
            returned = set(col.name for col in exp_returned_schema.columns)
            _validate_columns_present(
                act_returned_cols, returned, "returned_columns", error
            )
            exp_returned_schema.valid(tbl)
        elif exp_returned_columns != []:
            act_added_columns = act_returned_cols.difference(act_returned_cols_prior)
            if act_added_columns != set(exp_added_columns):
                msg = ""
                raise FootingsTaskExitError(msg)
            if act_returned_cols != exp_returned_columns:
                msg = ""
                raise FootingsTaskExitError(msg)
        else:
            act_removed_columns = act_returned_cols_prior.difference(act_returned_cols)
            if act_removed_columns != set(exp_removed_columns):
                msg = ""
                raise FootingsTaskExitError(msg)
            act_added_columns = act_returned_cols.difference(act_returned_cols_prior)
            if act_added_columns != set(col.name for col in exp_added_columns):
                msg = ""
                raise FootingsTaskExitError(msg)

    return test_exit_function


@create_task.register(method="tbl")
def _(name, function, **kwargs):
    """Class method to create FootingsTask."""
    required_columns = kwargs.pop("required_columns", [])
    removed_columns = kwargs.pop("removed_columns", [])
    added_columns = kwargs.pop("added_columns", [])
    returned_columns = kwargs.pop("returned_columns", [])
    returned_schema = kwargs.pop("returned_schema", None)
    if len(kwargs) > 0:
        msg = f"Non used items passed to kwargs [{list(kwargs.keys())}]."
        raise FootingsTaskCreationError(msg)
    if added_columns is not None:
        if not all([isinstance(col, ColSchema) for col in added_columns]):
            msg = ""
            raise FootingsTaskCreationError(msg)
    if returned_schema is not None:
        if not isinstance(returned_schema, TblSchema):
            msg = ""
            raise FootingsTaskCreationError(msg)
    test_enter_func = _create_test_enter_func(
        required_columns, removed_columns, added_columns, returned_columns
    )
    test_exit_func = _create_test_exit_func(
        removed_columns, added_columns, returned_columns, returned_schema
    )
    return FootingsTask(
        name=name, function=function, test_enter=test_enter_func, test_exit=test_exit_func
    )
