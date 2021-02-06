from attr import attrib, attrs

from .data_dictionary import DataDictionary

# from attr.validators import (
#     and_,
#     deep_iterable,
#     deep_mapping,
#     in_,
#     instance_of,
#     is_callable,
#     matches_re,
#     provides,
# )


__all__ = [
    "equal_to",
    "not_equal_to",
    "isin",
]

# "greater_than",
# "greater_than_or_equal_to",
# "less_than",
# "less_than_or_equal_to",
# "in_range",
# "is_in",
# "not_in",
# "str_contains",
# "str_ends_with",
# "str_length",
# "str_matches",
# "str_starts_with",
# "deep_iterable",
# "deep_mapping",
# "is_callable",


@attrs(repr=False, slots=True, hash=True)
class _EqualToValidator:
    value = attrib()

    def _call_data_dictionary(self, inst, attr, value):
        test = value != self.value
        fails = sum(test)
        if fails > 0:
            msg = f"{attr.name} failed {repr(self)[1:-1]} {fails} out of {str(len(value))} rows."
            raise ValueError(msg)

    def __call__(self, inst, attr, value):
        if isinstance(inst, DataDictionary):
            return self._call_data_dictionary(inst, attr, value)
        if value != self.value:
            msg = f"{attr.name} value of {str(value)} does not equal {str(self.value)}."
            raise ValueError(msg)

    def __repr__(self):
        return f"<equal_to(value={str(self.value)}) validator>"


def equal_to(value):
    """A validator that raises a `ValueError` if the initializer is called with a value
    that is not equal to the value provided.

    Can be used under both a `DataDictionary` and a `Model`.

    Parameters
    ----------
    value : Any
        The value to compare for equality.

    Raises
    ------
    ValueError
        With a human readable error message, the attribute (of type `attr.Attribute`), the
        equality value, and the value passed.
    """
    return _EqualToValidator(value)


@attrs(repr=False, slots=True, hash=True)
class _NotEqualToValidator:
    value = attrib()

    def _call_data_dictionary(self, inst, attr, value):
        test = value == self.value
        fails = sum(test)
        if fails > 0:
            msg = f"{attr.name} failed {repr(self)[1:-1]} {fails} out of {str(len(value))} rows."
            raise ValueError(msg)

    def __call__(self, inst, attr, value):
        if isinstance(inst, DataDictionary):
            return self._call_data_dictionary(inst, attr, value)
        if value == self.value:
            msg = f"{attr.name} value of {str(value)} does equal {str(self.value)}."
            raise ValueError(msg)

    def __repr__(self):
        return f"<not_equal_to(value={str(self.value)}) validator>"


def not_equal_to(value):
    """A validator that raises a `ValueError` if the initializer is called with a value
    that is equal to the value provided.

    Can be used under both a `DataDictionary` and a `Model`.

    Parameters
    ----------
    value : Any
        The value to compare for equality.

    Raises
    ------
    ValueError
        With a human readable error message, the attribute (of type `attr.Attribute`), the
        equality value, and the value passed.
    """
    return _NotEqualToValidator(value)


def isin(options):
    pass


def data_dictionary_types(data_dictionary):
    pass


def data_dictionary_validators(data_dictionary):
    pass


def data_dictionary_valid(data_dictionary):
    pass
