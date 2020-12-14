from footings.attributes import (
    def_return,
    def_meta,
    def_sensitivity,
    def_parameter,
    def_intermediate,
)
from footings.utils import dispatch_function
from footings.model import model, step

from ._version import get_versions

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions
