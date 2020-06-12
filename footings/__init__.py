"""__init__.py"""

from footings.argument import create_argument
from footings.footing import use
from footings.utils import create_dispatch_function, create_loaded_function
from footings.model import create_model

from ._version import get_versions

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions
