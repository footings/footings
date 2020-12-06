from footings.attributes import (
    define_asset,
    define_meta,
    define_modifier,
    define_parameter,
    define_placeholder,
)
from footings.utils import dispatch_function
from footings.model import Footing, model, step

from ._version import get_versions

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions
