import sys
from traceback import extract_tb, format_list

from attr import asdict, attrib, attrs
from attr.validators import instance_of


@attrs(frozen=True, slots=True)
class Error:
    """An exception as an object.

    :param key: The key identifying the point of error.
    :param error_type: The error type.
    :param error_value: The error value.
    :param error_stacktrace: The stacktrace of the error.
    """

    key = attrib(type=str, validator=instance_of(str))
    error_type = attrib(type=str, validator=instance_of(str))
    error_value = attrib(type=str, validator=instance_of(str))
    error_stacktrace = attrib(type=str, validator=instance_of(str))

    @classmethod
    def create(cls, key, sys_info):
        exc_type, exc_value, exc_trace = sys.exc_info()
        return cls(
            key=str(key),
            error_type=exc_type.__qualname__,
            error_value=str(exc_value.args),
            error_stacktrace=str(format_list(extract_tb(exc_trace))),
        )

    def to_audit_json(self):
        return asdict(self)

    def to_audit_xlsx(self):
        return asdict(self)


class ModelRunError(Exception):
    """Error occured while running a footings model."""


class ModelCreationError(Exception):
    """Error occured when creating a footings model."""


class DataDictionaryValidatorsConversionError(Exception):
    """Error occured when converting validator to Column."""


class DataDictionaryPandasDtypeConversionError(Exception):
    """Error occured when converting object to PandasDtype."""


class DataDictionaryValidateError(Exception):
    """Validator failed when testing whether dataframe is valid."""
