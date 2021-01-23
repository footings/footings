from footings.attributes import (
    def_return,
    def_meta,
    def_sensitivity,
    def_parameter,
    def_intermediate,
)
from footings.model import model, step
from ._version import get_versions
from . import audit, data_dictionary, jigs, utils, testing, validators

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions
