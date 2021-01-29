import pytest

from attr import Attribute, NOTHING
import pandas as pd

from footings import validators as validator_module
from footings.data_dictionary import data_dictionary, def_column

# from footings.exceptions import DataDictionaryValidateError
# from footings.model import model, def_parameter
from footings.validators import equal_to, not_equal_to


def simple_attr(name, validator=None):
    """
    Return an attribute with a name and no other bells and whistles.
    """
    return Attribute(
        name=name,
        default=NOTHING,
        validator=validator,
        repr=repr,
        cmp=None,
        eq=True,
        hash=hash,
        init=True,
        converter=None,
        kw_only=True,
        inherited=False,
    )


@pytest.fixture(scope="session")
def DD():
    @data_dictionary
    class TestDD:
        COL = def_column()

    return TestDD


def _make_df(col_vals, dtype):
    return pd.DataFrame({"COL": pd.Series(col_vals, dtype=dtype)})


class TestEqualTo:
    """Tests for `equal_to`."""

    def test_in_all(self):
        assert equal_to.__name__ in validator_module.__all__

    def test_data_dictionary_fail(self, DD):
        vald = equal_to(1)
        df = _make_df([1, 1, 3], int)

        # using _call_data_dictionary
        with pytest.raises(ValueError) as call_dd:
            vald._call_data_dictionary(DD, simple_attr("COL"), df["COL"])

        # using __call__
        with pytest.raises(ValueError) as call:
            vald(DD, simple_attr("COL"), df["COL"])

        assert call.value.args == call_dd.value.args
        assert call.value.args == (
            "COL failed equal_to(value=1) validator 1 out of 3 rows.",
        )

    def test_data_dictionary_success(self, DD):
        vald = equal_to(1)
        df = _make_df([1, 1, 1], int)

        # using _call_data_dictionary
        assert vald._call_data_dictionary(DD, simple_attr("COL"), df["COL"]) is None

        # using __call__
        assert vald(DD, simple_attr("COL"), df["COL"]) is None

    def test_base_fail(self):
        vald = equal_to(1)
        with pytest.raises(ValueError) as e:
            vald(None, simple_attr("test"), 2)

        assert e.value.args == ("test value of 2 does not equal 1.",)

    def test_base_success(self):
        vald = equal_to(1)
        assert vald(None, simple_attr("test"), 1) is None

    def test_repr(self):
        assert repr(equal_to(1)) == "<equal_to(value=1) validator>"


class TestNotEqualTo:
    """Tests for `not_equal_to`."""

    def test_in_all(self):
        assert not_equal_to.__name__ in validator_module.__all__

    def test_data_dictionary_fail(self, DD):
        vald = not_equal_to(1)
        df = _make_df([1, 1, 3], int)

        # using _call_data_dictionary
        with pytest.raises(ValueError) as call_dd:
            vald._call_data_dictionary(DD, simple_attr("COL"), df["COL"])

        # using __call__
        with pytest.raises(ValueError) as call:
            vald(DD, simple_attr("COL"), df["COL"])

        assert call.value.args == call_dd.value.args
        assert call.value.args == (
            "COL failed not_equal_to(value=1) validator 2 out of 3 rows.",
        )

    def test_data_dictionary_success(self, DD):
        vald = not_equal_to(1)
        df = _make_df([3, 3, 3], int)

        # using _call_data_dictionary
        assert vald._call_data_dictionary(DD, simple_attr("COL"), df["COL"]) is None

        # using __call__
        assert vald(DD, simple_attr("COL"), df["COL"]) is None

    def test_base_fail(self):
        vald = not_equal_to(1)
        with pytest.raises(ValueError) as e:
            vald(None, simple_attr("test"), 1)

        assert e.value.args == ("test value of 1 does equal 1.",)

    def test_base_success(self):
        vald = not_equal_to(1)
        assert vald(None, simple_attr("test"), 2) is None

    def test_repr(self):
        assert repr(not_equal_to(1)) == "<not_equal_to(value=1) validator>"


def test_not_equal_to():
    pass


def test_greater_than():
    pass


def test_greater_than_or_equal_to():
    pass


def test_less_than():
    pass


def test_less_than_or_equal_to():
    pass


def test_in_range():
    pass


def test_test_is_in():
    pass


def test_not_in():
    pass


def test_str_contains():
    pass


def test_str_ends_with():
    pass


def test_str_length():
    pass


def test_str_matches():
    pass


def test_str_starts_with():
    pass


def test_data_dictionary_types():
    pass


def test_data_dictionary_validators():
    pass


def test_data_dictionary_valid():
    pass
