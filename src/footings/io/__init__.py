import pathlib

from ..utils import dispatch_function
from .from_json import load_footings_json_file
from .from_xlsx import load_footings_xlsx_file
from .to_json import create_footings_json_file
from .to_xlsx import create_footings_xlsx_file


def load_footings_file(file: str):
    """Load footings generated file.

    Parameters
    ----------
    file : str
        The path to the file.

    Returns
    -------
    dict
        A dict representing the respective file type.

    See Also
    --------
    load_footings_json_file
    load_footings_xlsx_file
    """
    file_ext = pathlib.Path(file).suffix
    return _load_footings_file(file_ext=file_ext, file=file)


@dispatch_function(key_parameters=("file_ext",))
def _load_footings_file(file_ext, file):
    msg = f"No registered function to load a file with extension {file_ext}."
    raise NotImplementedError(msg)


@_load_footings_file.register(file_ext=".json")
def _(file):
    return load_footings_json_file(file)


@_load_footings_file.register(file_ext=".xlsx")
def _(file: str):
    return load_footings_xlsx_file(file)
