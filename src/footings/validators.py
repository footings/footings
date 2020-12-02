from attr import attrs, attrib


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
        return f"<value validator using {self.test} test with value {str(self.value)}>"


def min_val_validator(value):
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


def max_val_validator(value):
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


def min_len_validator(value):
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


def max_len_validator(value):
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


@attrs(repr=False, slots=True, hash=True)
class _InstanceCustomValidator:
    custom = attrib()

    def __call__(self, inst, attr, value):
        if self.custom(value) is False:
            msg = f"The custom validator failed using {value}."
            raise ValueError(msg, attr, self, value)

    def __repr__(self):
        return f"<custom validator for {str(self.custom)}>"


def custom_validator(custom: callable):
    """A custom validator.

    Parameters
    ----------
    custom : callable
        A custom callable that returns true or false when a value is passed to it.

    Raises
    ------
    ValueError
        If a passed attribute failes when passed into the custom function.
    """
    return _InstanceCustomValidator(custom=custom)
