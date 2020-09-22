"""The provided objects under ``footings.core`` are the core objects used to build models using the Footings framework.

They are exposed under ``footings``. Thus, when loading these objects it is not necessary to include ``.core``.
"""

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
