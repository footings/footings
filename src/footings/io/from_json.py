import json
from operator import add
from typing import Mapping


def flatten_dict(d):
    results = []

    def lift(x):
        return f"/{str(x)}"

    def visit(subdict, results, partial_key=None):
        for k, v in subdict.items():
            new_key = f"/{str(k)}" if partial_key is None else add(partial_key, lift(k))
            if isinstance(v, Mapping):
                visit(v, results, new_key)
            else:
                results.append((new_key, v))

    visit(d, results, None)
    return dict(results)


def load_footings_json_file(file: str):
    """Load footings generated json file.

    Parameters
    ----------
    file : str
        The path to the file.

    Returns
    -------
    dict
        A dict representing the respective file type.
    """
    with open(file, "r") as f:
        loaded_json = json.load(f)
    return flatten_dict(loaded_json)
