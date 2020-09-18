from footings.core.attributes import (
    define_asset,
    define_meta,
    define_modifier,
    define_parameter,
    define_placeholder,
)
from footings.core.utils import dispatch_function
from footings.core.model import Footing, model, step

from ._version import get_versions

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions
