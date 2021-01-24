from ._version import get_versions
from . import model, audit, data_dictionary, jigs, utils, testing, validators

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions
