"""functions.py"""

from functools import partial, singledispatch
from inspect import getfullargspec

from attr import attrs, attrib
from attr.validators import optional, is_callable, instance_of
import pandas as pd
from pandas.io.json import build_table_schema

from .schema import TblSchema, ColSchema

#########################################################################################
# errors
#########################################################################################


class FootingsNestedFunctionError(Exception):
    """Error creating nested function."""


class FootingsTblFunctionCreationError(Exception):
    """Error raised creating TblFunction."""


class FootingsTblFunctionEnterError(Exception):
    """Error raised running TblFunction on enter."""


class FootingsTblFunctionExitError(Exception):
    """Error raised running TblFunction on exit."""


class FootingsTblFunctionCallError(Exception):
    """Error raised tyring to call TblFunction."""


#########################################################################################
# NestedFunction
#########################################################################################


def _post_function_validator(instance, attribute, value):
    args = getfullargspec(value).args
    if len(args) > 1:
        msg = "The post_function callable can only take one argument. See documentation."
        raise FootingsNestedFunctionError(msg)


def _func_remove_columns(columns):
    """Remove columns"""

    def return_func(tbl):
        if isinstance(tbl, pd.DataFrame):
            return tbl.drop(columns, axis=1)
        raise NotImplementedError(f"Not implemented for tbl type [{type(tbl)}].")

    return return_func


@attrs(slots=True, frozen=True)
class NestedFunction:
    """Post hook"""

    function = attrib(validator=is_callable())
    post_function = attrib(validator=[is_callable(), _post_function_validator])

    def __call__(self, *args, **kwargs):
        return self.post_function(self.function(*args, **kwargs))


def make_nested(function=None, custom=None, remove_columns=None):
    """Function or decorator to create a nested function."""
    if custom is None and remove_columns is None:
        msg = "Custom and remove_columns cannot both be None."
        raise FootingsNestedFunctionError(msg)
    if custom is not None and remove_columns is not None:
        msg = "Custom and remove_columns cannot both be NOT None."
        raise FootingsNestedFunctionError(msg)
    if remove_columns is not None:
        post_function = _func_remove_columns(remove_columns)
    else:
        post_function = custom

    if function is None:
        return partial(NestedFunction, post_function=post_function)
    return NestedFunction(function=function, post_function=post_function)


#########################################################################################
# TblFunction
#########################################################################################


@singledispatch
def get_schema(tbl):
    """get_schema"""
    raise NotImplementedError(f"get_schema has not been defined for [{type(tbl)}].")


@get_schema.register(pd.DataFrame)
def _(tbl):
    int_schema = build_table_schema(tbl)["fields"]
    return [(x["name"], x["type"]) for x in int_schema]


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
    error = FootingsTblFunctionEnterError

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
    error = FootingsTblFunctionExitError

    def test_exit_function(tbl, tbl_schema_enter):
        act_tbl_schema = get_schema(tbl)
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
                raise FootingsTblFunctionExitError(msg)
            if act_returned_cols != exp_returned_columns:
                msg = ""
                raise FootingsTblFunctionExitError(msg)
        else:
            act_removed_columns = act_returned_cols_prior.difference(act_returned_cols)
            if act_removed_columns != set(exp_removed_columns):
                msg = ""
                raise FootingsTblFunctionExitError(msg)
            act_added_columns = act_returned_cols.difference(act_returned_cols_prior)
            if act_added_columns != set(col.name for col in exp_added_columns):
                msg = ""
                raise FootingsTblFunctionExitError(msg)

    return test_exit_function


@attrs(slots=True, frozen=True, repr=False)
class TblFunction:
    """TblFunction"""

    tbl_name = attrib(validator=instance_of(str))
    function = attrib(validator=is_callable())
    test_enter = attrib(default=None, validator=optional(is_callable()))
    test_exit = attrib(default=None, validator=optional(is_callable()))

    @classmethod
    def create(cls, tbl_name, function, **kwargs):
        """Class method to create TblFunction."""
        required_columns = kwargs.pop("required_columns", [])
        removed_columns = kwargs.pop("removed_columns", [])
        added_columns = kwargs.pop("added_columns", [])
        returned_columns = kwargs.pop("returned_columns", [])
        returned_schema = kwargs.pop("returned_schema", None)

        if len(kwargs) > 0:
            msg = f"Non used items passed to kwargs [{list(kwargs.keys())}]."
            raise FootingsTblFunctionCreationError(msg)

        if added_columns is not None:
            if not all([isinstance(col, ColSchema) for col in added_columns]):
                msg = ""
                raise FootingsTblFunctionCreationError(msg)

        if returned_schema is not None:
            if not isinstance(returned_schema, TblSchema):
                msg = ""
                raise FootingsTblFunctionCreationError(msg)

        test_enter_func = _create_test_enter_func(
            required_columns, removed_columns, added_columns, returned_columns
        )

        test_exit_func = _create_test_exit_func(
            removed_columns, added_columns, returned_columns, returned_schema
        )

        return cls(
            tbl_name=tbl_name,
            function=function,
            test_enter=test_enter_func,
            test_exit=test_exit_func,
        )

    def __call__(self, **kwargs):
        tbl = kwargs.pop(self.tbl_name, None)
        if tbl is None:
            msg = f"The table [{self.tbl_name}] was not detected in kwargs."
            raise FootingsTblFunctionCallError(msg)
        self.test_enter(tbl)
        tbl_schema_enter = get_schema(tbl)
        tbl = self.function(tbl, **kwargs)
        self.test_exit(tbl, tbl_schema_enter)
        return tbl
