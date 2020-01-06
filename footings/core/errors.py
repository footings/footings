class ParameterDuplicateError(Exception):
    """Duplicate paremeter error"""

    pass


class ParameterNotKnownError(Exception):
    """Parameter passed is not known"""

    pass


class InputsTypeError(TypeError):
    """Type of value passed to inputs not allowed"""

    pass


class InputsValueError(ValueError):
    """Value passed to inputs not allowed"""

    pass


class OutputsTypeError(TypeError):
    """Type of value passed to outputs not allowed"""

    pass


class OutputsValueError(ValueError):
    """Value passed to outputs not allowed"""

    pass
