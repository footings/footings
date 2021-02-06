import json
import math
from datetime import date, datetime
from inspect import isclass

import numpy as np
import pandas as pd

# from ..exceptions import Error


def _set_key(k):
    if type(k) not in [str, int, float, bool, None]:
        return str(k)
    return k


def _column_to_list(col):
    def _set_val(val):
        if isinstance(val, (int, float, bool)):
            if math.isnan(val):
                return None
            else:
                return val
        return val

    col = col.to_list()
    if any(math.isnan(val) for val in col if isinstance(val, (int, float, bool))):
        col = [_set_val(val) for val in col]
    return col


class AuditJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, "to_audit_json"):
            return obj.to_audit_json()
        elif isinstance(obj, (int, float, bool)):
            if math.isnan(obj):
                return None
            return obj
        elif isinstance(obj, np.generic):
            return obj.item()
        elif isinstance(obj, pd.DataFrame):
            return {col: _column_to_list(obj[col]) for col in obj.columns}
        elif isinstance(obj, pd.Series):
            return {obj.name: _column_to_list(obj)}
        elif isinstance(obj, (pd.Timestamp, date, datetime)):
            return str(obj)
        # elif isinstance(obj, Error):
        #     return obj.to_audit_json()
        elif callable(obj):
            return "callable: " + obj.__module__ + "." + obj.__qualname__
        elif isclass(obj) and issubclass(obj, Exception):
            return obj.__name__
        else:
            return super().default(obj)


def create_footings_json_file(d, file, **kwargs):
    with open(file, "w") as data_file:
        json.dump(obj=d, fp=data_file, cls=AuditJSONEncoder, indent=2, **kwargs)
