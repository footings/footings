from footings.attributes import (
    define_return,
    define_meta,
    define_sensitivity,
    define_parameter,
    define_intermediate,
)
from footings.utils import dispatch_function
from footings.model import Footing, model, step

from ._version import get_versions

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions
