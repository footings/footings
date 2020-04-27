"""__init__.py"""

from footings.argument import create_argument
from footings.footing import Footing, use, create_footing_from_list
from footings.utils import DispatchFunction, LoadedFunction, create_loaded_function
from footings.model import build_model

__version__ = "0.1.0"
