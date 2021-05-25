from . import audit, data_dictionary, factories, jigs, model, testing, utils, validators

try:
    import importlib.metadata as importlib_metadata
except ModuleNotFoundError:
    import importlib_metadata

__version__ = importlib_metadata.version(__name__)
