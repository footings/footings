import re
from functools import partial

import numpy as np
import pandas as pd
import pytest
from attr import asdict

from footings.data_dictionary import (
    Column,
    DataDictionary,
    PandasDtype,
    data_dictionary,
    def_column,
)
from footings.exceptions import (
    DataDictionaryPandasDtypeConversionError,
    DataDictionaryValidateError,
    DataDictionaryValidatorsConversionError,
)
from footings.model import model
from footings.validators import equal_to


def clean_str(x):
    return re.sub(r"\s+", "", x, flags=re.UNICODE)


class TestColumn:
    """Test Column class"""

    def test_instance(self):
        assert isinstance(Column(name="COL"), Column)

    def test_kw_only(self):
        with pytest.raises(TypeError):
            Column("COL")

    def test_attributes(self):
        col1 = Column(name="COL")
        assert col1.dtype is None
        assert col1.description is None
        assert col1.validator == []
        assert col1.metadata == {}

        col2 = Column(name="COL", dtype="bool", description="COL description...")
        assert col2.dtype is PandasDtype.Bool
        assert col2.description == "COL description..."
        assert col2.validator == []
        assert col2.metadata == {}

    def test_dtype(self):
        # test converter_pandas_dtype
        assert Column(name="COL").dtype is None
        assert Column(name="COL", dtype="bool").dtype is PandasDtype.Bool
        assert Column(name="COL", dtype="Bool").dtype is PandasDtype.Bool
        assert Column(name="COL", dtype=PandasDtype.Bool).dtype is PandasDtype.Bool
        with pytest.raises(DataDictionaryPandasDtypeConversionError):
            Column(name="COL", dtype="x")

    def test_validator(self):
        # test converter_validators
        def val1():
            pass

        def val2():
            pass

        assert Column(name="COL", validator=val1).validator == [val1]
        assert Column(name="COL", validator=[val1]).validator == [val1]
        assert Column(name="COL", validator=[val1, val2]).validator == [val1, val2]
        with pytest.raises(NotImplementedError):
            # lookup validator not implemented yet
            Column(name="COL", validator="get_validator")
        with pytest.raises(DataDictionaryValidatorsConversionError):
            # validator needs to be callable
            Column(name="COL", validator=1)

    def test_metadata(self):
        meta = {"extra": "info"}
        assert Column(name="COL", metadata=meta).metadata == meta
        with pytest.raises(TypeError):
            Column(name="COL", metadata="x")  # metadata needs to be mapping


def test_def_column():
    col = def_column(dtype="bool")
    assert isinstance(col, partial)
    assert col.keywords == {
        "dtype": "bool",
        "description": None,
        "validator": [],
        "metadata": {},
    }


@pytest.fixture(scope="session", name="DD")
def dd_success():
    @data_dictionary
    class TestDD:
        COL1 = def_column(
            dtype="int64", description="This is column 1.", validator=equal_to(1)
        )
        COL2 = def_column(dtype="str", description="This is column 2.")

    return TestDD


@pytest.fixture(scope="session")
def df_correct():
    df = pd.DataFrame(
        {
            "COL1": pd.Series([1, 1, 1], dtype="int64"),
            "COL2": pd.Series(["a", "b", "c"], dtype="string"),
        }
    )
    return df


@pytest.fixture(scope="session")
def df_missing_column():
    df = pd.DataFrame({"COL1": pd.Series([1, 1, 1], dtype="int64")})
    return df


@pytest.fixture(scope="session")
def df_extra_column():
    df = pd.DataFrame(
        {
            "COL1": pd.Series([1, 1, 1], dtype="int64"),
            "COL2": pd.Series(["a", "b", "c"], dtype="string"),
            "COL3": pd.Series([1, 1, 1], dtype="int64"),
        }
    )
    return df


@pytest.fixture(scope="session")
def df_wrong_type():
    df = pd.DataFrame(
        {
            "COL1": pd.Series([1, 1, 1], dtype="int64"),
            "COL2": pd.Series([1, 1, 1], dtype="int64"),
        }
    )
    return df


@pytest.fixture(scope="session")
def df_fail_validator():
    df = pd.DataFrame(
        {
            "COL1": pd.Series([1, 1, 2], dtype="int64"),
            "COL2": pd.Series(["a", "b", "c"], dtype="string"),
        }
    )
    return df


@pytest.fixture(scope="session")
def df_wrong_multiple():
    df = pd.DataFrame(
        {
            "COL1": pd.Series([1, 1, 2], dtype="int64"),
            "COL2": pd.Series([1, 1, 1], dtype="int64"),
            "COL3": pd.Series([1, 1, 1], dtype="int64"),
        }
    )
    return df


@pytest.fixture(scope="session")
def df_all_dtypes():
    df = pd.DataFrame(
        {
            "Bool": pd.Series([], dtype="bool"),  #: ``"bool"`` numpy dtype
            "DateTime": pd.Series(
                [], dtype="datetime64[ns]"
            ),  #: ``"datetime64[ns]"`` numpy dtype
            "Timedelta": pd.Series(
                [], dtype="timedelta64[ns]"
            ),  #: ``"timedelta64[ns]"`` numpy dtype
            "Category": pd.Series(
                [], dtype="category"
            ),  #: pandas ``"categorical"`` datatype
            "Float": pd.Series([], dtype="float"),  #: ``"float"`` numpy dtype
            "Float16": pd.Series([], dtype="float16"),  #: ``"float16"`` numpy dtype
            "Float32": pd.Series([], dtype="float32"),  #: ``"float32"`` numpy dtype
            "Float64": pd.Series([], dtype="float64"),  #: ``"float64"`` numpy dtype
            "Int": pd.Series([], dtype="int"),  #: ``"int"`` numpy dtype
            "Int8": pd.Series([], dtype="int8"),  #: ``"int8"`` numpy dtype
            "Int16": pd.Series([], dtype="int16"),  #: ``"int16"`` numpy dtype
            "Int32": pd.Series([], dtype="int32"),  #: ``"int32"`` numpy dtype
            "Int64": pd.Series([], dtype="int64"),  #: ``"int64"`` numpy dtype
            "UInt8": pd.Series([], dtype="uint8"),  #: ``"uint8"`` numpy dtype
            "UInt16": pd.Series([], dtype="uint16"),  #: ``"uint16"`` numpy dtype
            "UInt32": pd.Series([], dtype="uint32"),  #: ``"uint32"`` numpy dtype
            "UInt64": pd.Series([], dtype="uint64"),  #: ``"uint64"`` numpy dtype
            "INT8": pd.Series(
                [], dtype="Int8"
            ),  #: ``"Int8"`` pandas dtype:: pandas 0.24.0+
            "INT16": pd.Series(
                [], dtype="Int16"
            ),  #: ``"Int16"`` pandas dtype: pandas 0.24.0+
            "INT32": pd.Series(
                [], dtype="Int32"
            ),  #: ``"Int32"`` pandas dtype: pandas 0.24.0+
            "INT64": pd.Series(
                [], dtype="Int64"
            ),  #: ``"Int64"`` pandas dtype: pandas 0.24.0+
            "UINT8": pd.Series(
                [], dtype="UInt8"
            ),  #: ``"UInt8"`` pandas dtype: pandas 0.24.0+
            "UINT16": pd.Series(
                [], dtype="UInt16"
            ),  #: ``"UInt16"`` pandas dtype: pandas 0.24.0+
            "UINT32": pd.Series(
                [], dtype="UInt32"
            ),  #: ``"UInt32"`` pandas dtype: pandas 0.24.0+
            "UINT64": pd.Series(
                [], dtype="UInt64"
            ),  #: ``"UInt64"`` pandas dtype: pandas 0.24.0+
            "Object": pd.Series([], dtype="object"),  #: ``"object"`` numpy dtype
            "Complex": pd.Series([], dtype="complex"),  #: ``"complex"`` numpy dtype
            "Complex64": pd.Series([], dtype="complex64"),  #: ``"complex"`` numpy dtype
            "Complex128": pd.Series([], dtype="complex128"),  #: ``"complex"`` numpy dtype
            # "Complex256": pd.Series([], dtype="complex256"),  #: ``"complex"`` numpy dtype
            "String": pd.Series([], dtype="string"),  #: ``"str"`` numpy dtype
        }
    )
    return df


@pytest.fixture(scope="session")
def dd_all_dtypes():
    @data_dictionary
    class TestAllDtypes:
        Bool = def_column(dtype="Bool")
        DateTime = def_column(dtype="DateTime")
        Timedelta = def_column(dtype="Timedelta")
        Category = def_column(dtype="Category")
        Float = def_column(dtype="Float")
        Float16 = def_column(dtype="Float16")
        Float32 = def_column(dtype="Float32")
        Float64 = def_column(dtype="Float64")
        Int = def_column(dtype="Int")
        Int8 = def_column(dtype="Int8")
        Int16 = def_column(dtype="Int16")
        Int32 = def_column(dtype="Int32")
        Int64 = def_column(dtype="Int64")
        UInt8 = def_column(dtype="UInt8")
        UInt16 = def_column(dtype="UInt16")
        UInt32 = def_column(dtype="UInt32")
        UInt64 = def_column(dtype="UInt64")
        INT8 = def_column(dtype="INT8")
        INT16 = def_column(dtype="INT16")
        INT32 = def_column(dtype="INT32")
        INT64 = def_column(dtype="INT64")
        UINT8 = def_column(dtype="UINT8")
        UINT16 = def_column(dtype="UINT16")
        UINT32 = def_column(dtype="UINT32")
        UINT64 = def_column(dtype="UINT64")
        Object = def_column(dtype="Object")
        Complex = def_column(dtype="Complex")
        Complex64 = def_column(dtype="Complex64")
        Complex128 = def_column(dtype="Complex128")
        # Complex256 = def_column(dtype="Complex256")
        String = def_column(dtype="String")

    return TestAllDtypes


def compare_attributes(result, expected):
    result = asdict(result)
    del result["counter"]
    expected = asdict(expected)
    del expected["counter"]
    return result == expected


class TestDataDictionary:
    """Test for data dictionary"""

    def test_instance(self, DD):
        assert isinstance(DD, DD.__class__)
        assert isinstance(DD, DataDictionary)

    def test_subclass(self, DD):
        assert issubclass(DD.__class__, DataDictionary)

    def test_columns(self, DD):
        assert DD.columns == ("COL1", "COL2",)

    def test_list_columns(self, DD):
        assert DD.list_columns() == [getattr(DD, "COL1"), getattr(DD, "COL2")]

    def test_cols_valid(self, DD, df_correct, df_extra_column, df_missing_column):
        assert DD._cols_valid(df_correct) == ([True, True], [],)
        assert DD._cols_valid(df_extra_column) == (
            [True, True, False],
            [
                "The column [COL3] is missing from the DataDictionary but is in the dataframe."
            ],
        )
        assert DD._cols_valid(df_missing_column) == (
            [True, False],
            [
                "The column [COL2] is missing from the dataframe but is in the DataDictionary."
            ],
        )

    def test_types_valid(self, DD, df_correct, df_wrong_type):

        assert DD._types_valid(df_correct) == ([True, True], [],)
        assert DD._types_valid(df_wrong_type) == (
            [True, False],
            [
                "".join(
                    [
                        "The column [COL2] in the DataDictionary has type [string] which ",
                        "is different from type in the dataframe passed [int64].",
                    ]
                )
            ],
        )

    def test_all_dtypes(self, df_all_dtypes, dd_all_dtypes):
        assert dd_all_dtypes.validate(df_all_dtypes)

    def test_validators_valid(self, DD, df_correct, df_fail_validator):
        assert DD._validators_valid(df_correct) == ([True], [],)
        assert DD._validators_valid(df_fail_validator) == (
            [False],
            ["The column [COL1] failed equal_to(value=1) validator."],
        )

    def test_validate(self, DD, df_correct, df_wrong_multiple):
        assert DD.validate(df_correct) is True
        with pytest.raises(DataDictionaryValidateError) as e:
            DD.validate(df_wrong_multiple)
        res_str = f"{clean_str(str(e.value))}"
        errors = "\n".join(
            [
                "The column [COL3] is missing from the DataDictionary but is in the dataframe.",
                "".join(
                    [
                        "The column [COL2] in the DataDictionary has type [string]",
                        " which is different from type in the dataframe passed [int64].",
                    ]
                ),
                "The column [COL1] failed equal_to(value=1) validator.",
            ]
        )
        exp_str = clean_str(errors)
        assert res_str == exp_str
        pytest.raises(DataDictionaryValidateError, DD.validate, df_wrong_multiple)

    def test_def_parameter(self, DD):
        @model
        class TestDD:
            col1 = DD.def_parameter("COL1")
            col1_mapping = DD.def_parameter("COL1", dtype_mapping={PandasDtype.Int64: str})
            col2 = DD.def_parameter("COL2")
            col2_validator = DD.def_parameter("COL2", add_dtype_validator=True)

        assert TestDD.__attrs_attrs__.col1.type == np.int64
        assert TestDD.__attrs_attrs__.col1_mapping.type == str
        assert TestDD.__attrs_attrs__.col2.type == str
        assert TestDD.__attrs_attrs__.col2_validator.type == str
        assert isinstance(
            TestDD(col1=1, col1_mapping=1, col2=1, col2_validator="x"), TestDD
        )
        with pytest.raises(TypeError):  # fails due col2_validator should be 
            TestDD(col1=1, col1_mapping=1, col2=1, col2_validator=1)
        with pytest.raises(ValueError):  # fails because col1 is 2
            TestDD(col1=2, col1_mapping=1, col2="x", col2_validator="x")
        with pytest.raises(ValueError):  # fails because co1_mapping is 2
            TestDD(col1=1, col1_mapping=2, col2="x", col2_validator="x")

    def test_def_sensitivity(self, DD):
        pytest.raises(NotImplementedError, DD.def_sensitivity, "COL1")

    def test_def_meta(self, DD):
        pytest.raises(NotImplementedError, DD.def_meta, "COL1")

    def test_def_column(self, DD):
        pytest.raises(AttributeError, DD.def_column, "zz")
        assert DD.def_column("COL1") == Column(
            name="COL1",
            dtype="int64",
            description="This is column 1.",
            validator=[equal_to(1)],
        )
        assert DD.def_column("COL2") == Column(
            name="COL2", dtype="string", description="This is column 2."
        )

    def test_repr(self, DD):
        pass

    #    repr_str = """TestDD[DataDictionary]
    #    Column(name='COL1', dtype=<PandasDtype.Int: 'int64'>, description='This is column 1.', validator=
    #    [<equal_to(value=1) validator>], metadata={})
    #    Column(name='COL2', dtype=<PandasDtype.String: 'string'>, description='This is column 2.', validator=[],
    #    metadata={})
    #    """
    #    assert clean_str(repr(DD)) == clean_str(repr_str)
