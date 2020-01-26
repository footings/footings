"""Objects tied to ffunctions"""

from functools import singledispatch, partial
from inspect import getfullargspec
from typing import Callable, Dict, Union, Tuple, List

from attr import attrs, attrib
from attr.validators import (
    instance_of,
    deep_iterable,
    deep_mapping,
    is_callable,
    optional,
)
import pandas as pd

from .table_schema import ColumnSchema
from .parameter import Parameter


__all__ = ["ffunction"]


class ColumnNotInTableError(Exception):
    """Column(s) missing from table"""


class ColumnInTableError(Exception):
    """Column(s) in table when they should not be"""


class FFunctionMissingArgumentError(Exception):
    """Argument missing in input for ffunction"""


class FFunctionExtraArgumentError(Exception):
    """Extra argument in input for ffunction"""


@singledispatch
def _check_columns_in_table(table, columns):
    raise NotImplementedError("table type not implemented")


@_check_columns_in_table.register(pd.DataFrame)
def _(table, columns):
    missing = set(columns) - set(table.columns)
    if len(missing) > 0:
        raise ColumnNotInTableError(f"The following columns are missing - {missing}")
    return True


@singledispatch
def _check_columns_not_in_table(table, columns):
    raise NotImplementedError("table type not implemented")


@_check_columns_not_in_table.register(pd.DataFrame)
def _(table, columns):
    present = set(columns).intersection(set(table.columns))
    if len(present) > 0:
        raise ColumnInTableError(f"The following columns are present - {present}")
    return True


@attrs(slots=True, frozen=True)
class TableIn:
    """Input table"""

    name: str = attrib(validator=instance_of(str))
    required_columns: list = attrib(validator=instance_of(list))

    def check_valid(self, table):
        """Check to see if table has required columns"""
        return _check_columns_in_table(table, self.required_columns)

    def create_validator(self):
        """Creates validator to be used to verify input table"""
        return self


@attrs(slots=True, frozen=True)
class TableOut:
    """Output table"""

    name: str = attrib()
    added_columns: List[ColumnSchema] = attrib(
        default=None, validator=optional(deep_iterable(instance_of(ColumnSchema)))
    )
    modified_columns: list = attrib(default=None, validator=optional(instance_of(list)))
    removed_columns: list = attrib(default=None, validator=optional(instance_of(list)))

    def check_valid(self, table):
        """Check to see if table has modified columns and not removed columns"""
        if self.added_columns:
            _check_columns_in_table(table, [c.name for c in self.added_columns])
        if self.modified_columns:
            _check_columns_in_table(table, self.modified_columns)
        if self.removed_columns:
            _check_columns_not_in_table(table, self.removed_columns)
        return True

    def create_validator(self):
        """Creates validator to be used to verify returned table"""
        return self


@attrs(slots=True, frozen=True, repr=False)
class FFunction:
    """ A callable object (i.e., function) fitted for use within the Footings framework.

    Parameters
    ----------
    function : callable
        The function to transform to a FFunction.
    inputs : dict
        Input dict
    outputs : dict

    Returns
    -------
    FFunction
        A callable object fitted for use within the Footings framework.

    Examples
    --------

    """

    function: Callable = attrib(validator=is_callable())
    inputs: Union[Dict[str, TableIn], Dict[str, Parameter]] = attrib(
        validator=deep_mapping(instance_of(str), instance_of((TableIn, Parameter)))
    )
    outputs: Union[TableOut, Tuple[TableOut]] = attrib(
        validator=instance_of(TableOut) or deep_iterable(instance_of(TableOut))
    )

    def __attrs_post_init__(self):
        required = set(getfullargspec(self.function).args)
        inputs = set(self.inputs.keys())
        missing = required - inputs
        if len(missing) > 0:
            msg = f"The following arguments are missing from the inputs - {missing}"
            raise FFunctionMissingArgumentError(msg)
        extra = inputs - required
        if len(extra) > 0:
            msg = f"The following input arguments are extras and are not needed - {extra}"
            raise FFunctionExtraArgumentError(msg)

    @property
    def name(self):
        """Returns name of function"""
        return self.function.__name__

    @property
    def input_parameters(self):
        """Returns dictionary of parameters"""
        return {k: v for k, v in self.inputs.items() if isinstance(v, Parameter)}

    @property
    def input_tables(self):
        """Returns dictonary of tables"""
        return {k: v for k, v in self.inputs.items() if isinstance(v, TableIn)}

    def get_step_items(self):
        """Get items for steps in dag"""
        args = getfullargspec(self.function).args
        in_ = tuple(self.inputs[arg].name for arg in args)
        if isinstance(self.outputs, tuple):
            out = (output.name for output in self.outputs)
        else:
            out = (self.outputs.name,)
        return out, self, in_

    def __call__(self, *args, **kwargs):
        return self.function(*args, **kwargs)


def ffunction(function=None, *, inputs=None, outputs=None):
    """A decorator for producing FFunctions"""
    if function is None:
        return partial(FFunction, inputs=inputs, outputs=outputs)
    return FFunction(function, inputs, outputs)
