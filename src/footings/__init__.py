from footings.core.parameter import define_parameter
from footings.core.footing import use
from footings.core.utils import dispatch_function, loaded_function
from footings.core.model import build_model

from ._version import get_versions

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions
