"""__init__.py"""

from footings.argument import create_argument
from footings.footing import Footing, use, create_footing_from_list
from footings.utils import DispatchFunction, LoadedFunction, create_loaded_function
from footings.model import build_model

from ._version import get_versions

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions
