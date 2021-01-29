from functools import partial
import re

from attr import asdict
import pandas as pd
import pytest

from footings.model import model
from footings.data_dictionary import (
    PandasDtype,
    Column,
    def_column,
    DataDictionary,
    data_dictionary,
)
from footings.validators import equal_to
from footings.exceptions import (
    DataDictionaryValidatorsConversionError,
    DataDictionaryPandasDtypeConversionError,
    DataDictionaryValidateError,
)


def clean_str(x):
    return re.sub(r"\s+", "", x, flags=re.UNICODE)


def create_df():

    df = pd.DataFrame()
    df["bool"] = pd.Series([True, False], dtype="bool")
    df["datetime64[ns]"] = pd.Series([0, 1], dtype="datetime64[ns]")
    df["timedelta64[ns]"] = pd.Series([0, 1], dtype="timedelta64[ns]")
    df["category"] = pd.Series(["a", "b"], dtype="category")
    df["float"] = pd.Series([1.1, 1.2], dtype="float")
    df["float16"] = pd.Series([1.1, 1.2], dtype="float16")
    df["float32"] = pd.Series([1.1, 1.2], dtype="float32")
    df["float64"] = pd.Series([1.1, 1.2], dtype="float64")
    df["int"] = pd.Series([1, 2], dtype="int")
    df["int8"] = pd.Series([1, 2], dtype="Int8")
    df["int16"] = pd.Series([1, 2], dtype="Int16")
    df["int32"] = pd.Series([1, 2], dtype="Int32")
    df["int64"] = pd.Series([1, 2], dtype="Int64")
    df["uint8"] = pd.Series([1, 2], dtype="UInt8")
    df["uint16"] = pd.Series([1, 2], dtype="UInt16")
    df["uint32"] = pd.Series([1, 2], dtype="UInt32")
    df["uint64"] = pd.Series([1, 2], dtype="UInt64")
    df["object"] = pd.Series(["a", "b"], dtype="object")
    df["complex"] = pd.Series([complex(1), complex(2)], dtype="complex")
    df["complex64"] = pd.Series([complex(1), complex(2)], dtype="complex64")
    df["complex128"] = pd.Series([complex(1), complex(2)], dtype="complex128")
    df["complex256"] = pd.Series([complex(1), complex(2)], dtype="complex256")
    df["string"] = pd.Series(["str1", "str2"], dtype="string")

    return df


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
            dtype="int", description="This is column 1.", validator=equal_to(1)
        )
        COL2 = def_column(dtype="str", description="This is column 2.")

    return TestDD


@pytest.fixture(scope="session")
def df_correct():
    df = pd.DataFrame(
        {
            "COL1": pd.Series([1, 1, 1], dtype=int),
            "COL2": pd.Series(["a", "b", "c"], dtype="string"),
        }
    )
    return df


@pytest.fixture(scope="session")
def df_missing_column():
    df = pd.DataFrame({"COL1": pd.Series([1, 1, 1], dtype=int),})
    return df


@pytest.fixture(scope="session")
def df_extra_column():
    df = pd.DataFrame(
        {
            "COL1": pd.Series([1, 1, 1], dtype=int),
            "COL2": pd.Series(["a", "b", "c"], dtype="string"),
            "COL3": pd.Series([1, 1, 1], dtype=int),
        }
    )
    return df


@pytest.fixture(scope="session")
def df_wrong_type():
    df = pd.DataFrame(
        {
            "COL1": pd.Series([1, 1, 1], dtype=int),
            "COL2": pd.Series([1, 1, 1], dtype=int),
        }
    )
    return df


@pytest.fixture(scope="session")
def df_fail_validator():
    df = pd.DataFrame(
        {
            "COL1": pd.Series([1, 1, 2], dtype=int),
            "COL2": pd.Series(["a", "b", "c"], dtype="string"),
        }
    )
    return df


@pytest.fixture(scope="session")
def df_wrong_multiple():
    df = pd.DataFrame(
        {
            "COL1": pd.Series([1, 1, 2], dtype=int),
            "COL2": pd.Series([1, 1, 1], dtype=int),
            "COL3": pd.Series([1, 1, 1], dtype=int),
        }
    )
    return df


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
            col1_mapping = DD.def_parameter("COL1", dtype_mapping={PandasDtype.Int: str})
            col2 = DD.def_parameter("COL2")
            col2_validator = DD.def_parameter("COL2", add_dtype_validator=True)

        assert TestDD.__attrs_attrs__.col1.type == int
        assert TestDD.__attrs_attrs__.col1_mapping.type == str
        assert TestDD.__attrs_attrs__.col2.type == str
        assert TestDD.__attrs_attrs__.col2_validator.type == str
        assert isinstance(
            TestDD(col1=1, col1_mapping=1, col2=1, col2_validator="x"), TestDD
        )
        with pytest.raises(TypeError):  # fails due col2_validator should be str
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
            dtype="int",
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
