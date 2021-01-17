from attr import attrs, attrib
from attr.validators import (
    and_,
    deep_iterable,
    deep_mapping,
    in_,
    instance_of,
    is_callable,
    matches_re,
    provides,
)


__all__ = [
    "value_min",
    "value_max",
    "value_in_range",
    "len_min",
    "len_max",
    "len_in_range",
    "and_",
    "deep_iterable",
    "deep_mapping",
    "in_",
    "instance_of",
    "is_callable",
    "matches_re",
    "provides",
]


@attrs(repr=False, slots=True, hash=True)
class _InstanceValueValidator:
    value = attrib()
    test = attrib()

    def __call__(self, inst, attr, value):
        error = False
        msg = ""
        if self.test == "min":
            if self.value > value:
                error = True
                msg += f"The value {value} is less than {self.value}."
        if self.test == "max":
            if self.value < value:
                error = True
                msg += f"The value {value} is greater than {self.value}."

        if error:
            raise ValueError(msg, attr, self, value)

    def __repr__(self):
        return (
            f"<value validator using {str(self.test)} test with value {str(self.value)}>"
        )


def value_min(value):
    """A validator to set as a minimum.

    Parameters
    ----------
    value : Any object with rich comparison methods
        The minimum to establish as a validator.

    Raises
    ------
    ValueError
        If a passed attribute is less than value.
    """
    return _InstanceValueValidator(value=value, test="min")


def value_max(value):
    """A validator to set as a maximum.

    Parameters
    ----------
    value : Any object with rich comparison methods
        The maximum to establish as a validator.

    Raises
    ------
    ValueError
        If a passed attribute is greater than value.
    """
    return _InstanceValueValidator(value=value, test="max")


def value_in_range(lower, upper):
    """ """
    pass


@attrs(repr=False, slots=True, hash=True)
class _InstanceLengthValidator:
    value = attrib()
    test = attrib()

    def __call__(self, inst, attr, value):
        error = False
        msg = ""
        if self.test == "min":
            if self.value > len(value):
                error = True
                msg += f"The value {value} has length less than {self.value}."
        if self.test == "max":
            if self.value < len(value):
                error = True
                msg += f"The value {value} has length greater than {self.value}."
        if error:
            raise ValueError(msg, attr, self, value)

    def __repr__(self):
        return f"<length validator using {self.test} test with value {str(self.value)}>"


def len_min(value):
    """A validator to set as a minimum length.

    Parameters
    ----------
    value : Any object with a __len__ method
        The minimum len to establish as a validator.

    Raises
    ------
    ValueError
        If a passed attribute has length less than value.
    """
    return _InstanceLengthValidator(value=value, test="min")


def len_max(value):
    """A validator to set as a maximum length.

    Parameters
    ----------
    value : Any object with a __len__ method
        The maximum len to establish as a validator.

    Raises
    ------
    ValueError
        If a passed attribute has length greater than value.
    """
    return _InstanceLengthValidator(value=value, test="max")


def len_in_range(lower, upper):
    pass


def schema_structure(schema):
    pass


def schema_all(schema):
    pass
