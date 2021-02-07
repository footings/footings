import platform
from enum import Enum
from functools import partial
from typing import List, Mapping, Optional, Union

import numpy as np
import pandas as pd
from attr import attrib, attrs, evolve, make_class
from attr.validators import instance_of, optional

from .exceptions import (
    DataDictionaryPandasDtypeConversionError,
    DataDictionaryValidateError,
    DataDictionaryValidatorsConversionError,
)
from .model import def_parameter

__all__ = ["PandasDtype", "data_dictionary", "def_column"]


class PandasDtype(Enum):
    """Enum container of all valid pandas data types.

    This was copied from the `pandera` project and modified.
    """

    Bool = "bool"
    DateTime = "datetime64[ns]"
    Timedelta = "timedelta64[ns]"
    Category = "category"
    Float = "float64"
    Float16 = "float16"
    Float32 = "float32"
    Float64 = "float64"
    Int = "int32" if platform.system() == "Windows" else "int64"
    Int8 = "int8"
    Int16 = "int16"
    Int32 = "int32"
    Int64 = "int64"
    UInt8 = "uint8"
    UInt16 = "uint16"
    UInt32 = "uint32"
    UInt64 = "uint64"
    INT8 = "Int8"
    INT16 = "Int16"
    INT32 = "Int32"
    INT64 = "Int64"
    UINT8 = "UInt8"
    UINT16 = "UInt16"
    UINT32 = "UInt32"
    UINT64 = "UInt64"
    Object = "object"
    Complex = "complex128"
    Complex64 = "complex64"
    Complex128 = "complex128"
    Complex256 = "complex256"
    String = "string"


PANDAS_DTYPE_STR_ALIAS = {
    "bool": PandasDtype.Bool,
    "datetime64[ns]": PandasDtype.DateTime,
    "timedelta64[ns]": PandasDtype.Timedelta,
    "category": PandasDtype.Category,
    "float": PandasDtype.Float,
    "float16": PandasDtype.Float16,
    "float32": PandasDtype.Float32,
    "float64": PandasDtype.Float64,
    "int": PandasDtype.Int,
    "int8": PandasDtype.Int8,
    "int16": PandasDtype.Int16,
    "int32": PandasDtype.Int32,
    "int64": PandasDtype.Int64,
    "uint8": PandasDtype.UInt8,
    "uint16": PandasDtype.UInt16,
    "uint32": PandasDtype.UInt32,
    "uint64": PandasDtype.UInt64,
    "Int8": PandasDtype.INT8,
    "Int16": PandasDtype.INT16,
    "Int32": PandasDtype.INT32,
    "Int64": PandasDtype.INT64,
    "UInt8": PandasDtype.UINT8,
    "UInt16": PandasDtype.UINT16,
    "UInt32": PandasDtype.UINT32,
    "UInt64": PandasDtype.UINT64,
    "object": PandasDtype.Object,
    "complex": PandasDtype.Complex,
    "complex64": PandasDtype.Complex64,
    "complex128": PandasDtype.Complex128,
    "complex256": PandasDtype.Complex256,
    "str": PandasDtype.String,
    "string": PandasDtype.String,
}


PANDAS_DTYPE_CONVERSION = {
    PandasDtype.Bool: bool,
    PandasDtype.DateTime: pd.Timestamp,
    PandasDtype.Timedelta: pd.Timedelta,
    PandasDtype.Category: str,
    PandasDtype.Float: float,
    PandasDtype.Float16: float,
    PandasDtype.Float32: float,
    PandasDtype.Float64: float,
    PandasDtype.Int: np.int32 if platform.system() == "Windows" else np.int64,
    PandasDtype.Int8: np.int8,
    PandasDtype.Int16: np.int16,
    PandasDtype.Int32: np.int32,
    PandasDtype.Int64: np.int64,
    PandasDtype.UInt8: np.uint8,
    PandasDtype.UInt16: np.uint16,
    PandasDtype.UInt32: np.uint32,
    PandasDtype.UInt64: np.uint64,
    PandasDtype.INT8: pd.Int8Dtype,
    PandasDtype.INT16: pd.Int16Dtype,
    PandasDtype.INT32: pd.Int32Dtype,
    PandasDtype.INT64: pd.Int64Dtype,
    PandasDtype.UINT8: pd.UInt8Dtype,
    PandasDtype.UINT16: pd.UInt16Dtype,
    PandasDtype.UINT32: pd.UInt32Dtype,
    PandasDtype.UINT64: pd.UInt64Dtype,
    PandasDtype.Object: str,
    PandasDtype.Complex: complex,
    PandasDtype.Complex64: complex,
    PandasDtype.Complex128: complex,
    PandasDtype.Complex256: complex,
    PandasDtype.String: str,
}


def converter_pandas_dtype(x: Union[str, PandasDtype]):
    if x is None:
        return None
    if isinstance(x, str):
        x_upd = getattr(PandasDtype, x, None)  # try to get attribute
        if x_upd is None:  # if attribute fails lookup string alias
            x_upd = PANDAS_DTYPE_STR_ALIAS.get(x, None)
        if x_upd is None:  # if both getting attribute and string alias fail raise error.
            msg = f"The passed string [{x}] is not a built-in PandasDtype or a valid string alias."
            raise DataDictionaryPandasDtypeConversionError(msg)
        return x_upd
    elif isinstance(x, PandasDtype) is False:
        msg = "The passed object is not a built-in PandasDtype or a valid string alias."
        raise DataDictionaryPandasDtypeConversionError(msg)
    return x


def converter_validators(value: Union[str, callable]):
    def inner(x):
        if isinstance(x, str):
            msg = "The feature to look up a validator by string name is not implemented yet."
            raise NotImplementedError(msg)
        elif not callable(x):
            msg = "The passed object needs to be a callable object."
            raise DataDictionaryValidatorsConversionError(msg)
        return x

    if isinstance(value, list):
        return [inner(x) for x in value]
    return [inner(value)]


@attrs(frozen=True, slots=True)
class Column:
    """Container for column kwargs to pass into `attrs.attrib`.

    :param str name: The name of the column.
    :param Optional[Union[str, PandasDtype]] dtype: The column data type.
    :param Optional[str] description: The column description.
    :param Optional[Union[List[callable], callable]] validator: Column validators.
    :param Optional[Mapping] metadata: Optional metadata to attach to column.
    """

    name = attrib(type=str, kw_only=True, validator=instance_of(str))
    dtype = attrib(
        type=Optional[Union[str, PandasDtype]],
        default=None,
        converter=converter_pandas_dtype,
        kw_only=True,
    )
    description = attrib(
        type=Optional[str],
        default=None,
        kw_only=True,
        validator=optional(instance_of(str)),
    )
    validator = attrib(
        type=Optional[Union[List[callable], callable]],
        factory=list,
        kw_only=True,
        converter=converter_validators,
    )
    metadata = attrib(
        type=Mapping, factory=dict, kw_only=True, validator=instance_of(Mapping)
    )

    def __str__(self):
        string = self.name + " : "
        if self.dtype is not None:
            string += str(self.dtype)
        string += "\n"
        if self.description is not None:
            string += "    " + self.description
        string += "\n"
        if len(self.validator) > 0:
            for val in self.validator:
                string += str(val)
            string += "\n"
        if len(self.metadata) > 0:
            string += str(self.metadata)
            string += "\n"
        return string


def def_column(
    dtype: Optional[Union[str, PandasDtype]] = None,
    description: Optional[str] = None,
    validator: Optional[Union[List[callable], callable]] = None,
    metadata: Optional[Mapping] = None,
) -> partial:
    """Define a column with a data dictionary.

    :param Optional[Union[str, PandasDtype]] dtype: The column data type.
    :param Optional[str] description: The column description.
    :param Optional[Union[List[callable], callable]] validator: Column validators.
    :param Optional[Mapping] metadata: Optional metadata to attach to column.

    :return: A partially completed Column object (missing name which is added
        the data dictioanry is created).
    :rtype: partial
    """

    if validator is None:
        validator = []
    if metadata is None:
        metadata = {}
    return partial(
        Column,
        dtype=dtype,
        description=description,
        validator=validator,
        metadata=metadata,
    )


@attrs(frozen=True, slots=True, repr=False)
class DataDictionary:
    """The parent class of a container that provides information on a tabular data structure."""

    __columns__ = attrib(init=False, repr=False)

    @property
    def columns(self):
        """Show column names."""
        return self.__columns__

    def list_columns(self):
        """List all columns."""
        return [getattr(self, col) for col in self.__columns__]

    # def __repr__(self):
    #     name = self.__class__.__qualname__
    #     cols = "\n\t".join([repr(getattr(self, col)) for col in self.__columns__])
    #     return f"{name}[DataDictionary]\n\t{cols}\n"

    def _cols_valid(self, dataframe: pd.DataFrame):
        """Test match of columns in datadictionary vs dataframe."""
        results, msgs = [], []
        col_dict = {col.name: col for col in self.list_columns()}
        cols = list(dict.fromkeys(list(col_dict) + list(dataframe.columns)))
        for col in cols:
            dd_col = col_dict.get(col, None)
            df_col = dataframe.get(col, None)
            if dd_col is None:
                results.append(False)
                msg = f"The column [{col}] is missing from the DataDictionary but is in the dataframe."
                msgs.append(msg)
            if df_col is None:
                results.append(False)
                msg = f"The column [{col}] is missing from the dataframe but is in the DataDictionary."
                msgs.append(msg)
            if dd_col is not None and df_col is not None:
                results.append(True)
        return results, msgs

    def _types_valid(self, dataframe: pd.DataFrame):
        """Test column types in dataframe."""
        results, msgs = [], []
        for dd_col in self.list_columns():
            df_col = dataframe.get(dd_col.name, None)
            if df_col is not None:
                test_eq = dd_col.dtype.value == df_col.dtype.name
                if test_eq is False:
                    msg = f"The column [{dd_col.name}] in the DataDictionary has type [{dd_col.dtype.value}] "
                    msg += f"which is different from type in the dataframe passed [{df_col.dtype.name}]."
                    msgs.append(msg)
                results.append(test_eq)

        return results, msgs

    def _validators_valid(self, dataframe: pd.DataFrame):
        """Test validators for columns in dataframe."""
        results, msgs = [], []
        for dd_col in self.list_columns():
            df_col = dataframe.get(dd_col.name, None)
            if df_col is not None and len(dd_col.validator) > 0:
                for validator in dd_col.validator:
                    try:
                        validator(inst=self, attr=dd_col, value=df_col)
                        results.append(True)
                    except:
                        results.append(False)
                        msgs.append(
                            f"The column [{dd_col.name}] failed {str(validator)[1:-1]}."
                        )

        return results, msgs

    def validate(
        self, dataframe: pd.DataFrame, types: bool = True, validators: bool = True
    ):
        """Validate passed dataframe types and/or validators against DataDictionary.

        :param pd.DataFrame dataframe: The dataframe to validate.
        :param bool types: Test DataDictionary types against dataframe. Default is True.
        :param bool validators: Test DataDictionary validators against dataframe. Default is True.

        :return: True if test pass.
        :rtype: bool

        :raises DataDictionaryValidateError: If any of the following rules are broken -
            - All columns present in the data dictionary are in the dataframe and vice versa.
            - If types is True, test the data types of the dataframe match the data dictionary.
            - If validators is True, test that any attached validator pass for a column.
        """
        rets, msgs = self._cols_valid(dataframe)

        def _update_ret_msg(fn, dataframe):
            ret, msg = fn(dataframe)
            rets.extend(ret)
            msgs.extend(msg)

        if types is True:
            _update_ret_msg(self._types_valid, dataframe)
        if validators is True:
            _update_ret_msg(self._validators_valid, dataframe)

        if not all(rets):
            msg = "\n".join(msgs)
            raise DataDictionaryValidateError("\n" + msg)
        return True

    def def_parameter(
        self,
        column: str,
        add_dtype_validator: bool = False,
        dtype_mapping: Mapping = PANDAS_DTYPE_CONVERSION,
    ):
        """Define a parameter for a model from a data dictioanry column.

        :param str column: The column name.
        :param bool add_type_validator: If True, an instance_of validator is added to the parameter.
        :param Mapping dtype_mapping: A conversion mapping of PandasDtypes to different types.
        """
        col = getattr(self, column, None)
        if col is None:
            msg = f"The column [{column}] does not belong to the data dictionary."
            raise AttributeError(msg)
        dtype = dtype_mapping.get(col.dtype)
        validator = col.validator.copy()
        if add_dtype_validator is True:
            validator.append(instance_of(dtype))
        return def_parameter(
            dtype=dtype, description=col.description, validator=validator,
        )

    def def_sensitivity(self, column):
        raise NotImplementedError("This feature is not implemented yet.")

    def def_meta(self, column):
        raise NotImplementedError("This feature is not implemented yet.")

    def def_column(self, column):
        """Define a column for another data dictionary using an existing data dictioanry column.

        :param str column: The column name.
        """
        col = getattr(self, column, None)
        if col is None:
            msg = f"The column [{column}] does not belong to the data dictionary."
            raise AttributeError(msg)
        return col


def make_data_doc(cls_doc, cols):
    if cls_doc is None:
        doc = ""
    else:
        doc = str(cls_doc) + "\n\n"
    doc += "Columns\n"
    doc += "-------\n"
    for col in cols:
        doc += str(col)
        doc += "\n"
    return doc


def data_dictionary(cls: type = None):
    """A decorator that creates a custom data dictionary object."""
    if cls is None:
        return partial(data_dictionary)

    def make_data_dictionary(cls):
        def set_column(x):
            col = getattr(cls, x)
            if isinstance(col, Column):
                return evolve(col, name=x)
            return col(name=x)

        exclude = [x for x in DataDictionary.__dict__.keys() if x[0] != "_"]
        cols = [
            set_column(x) for x in cls.__dict__.keys() if x[0] != "_" and x not in exclude
        ]
        attrs = {col.name: attrib(default=col, init=False) for col in cols}
        orig_doc = str(cls.__doc__)
        cls = make_class(
            cls.__name__,
            attrs,
            bases=(DataDictionary,),
            slots=True,
            frozen=True,
            repr=False,
        )
        cls.__columns__ = tuple(col.name for col in cols)
        cls.__doc__ = make_data_doc(orig_doc, cols)
        return cls

    return make_data_dictionary(cls)()  # note an instance is returned
