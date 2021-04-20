import json
import math
import re
from datetime import date, datetime
from inspect import isclass

import numpy as np
import pandas as pd


def _series_to_json(col):
    if hasattr(col, "dt"):
        if col.dt.hour.sum() == 0 and col.dt.minute.sum() == 0:
            col = col.dt.strftime("%Y-%m-%d")
    col_json = col.to_json(orient="records", date_format="iso")
    return f"@@{col_json}@@"


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
            return {col: _series_to_json(obj[col]) for col in obj.columns}
        elif isinstance(obj, pd.Series):
            return {obj.name: _series_to_json(obj)}
        elif isinstance(obj, pd.Timestamp):
            if obj.hour == 0 and obj.minute == 0:
                return str(obj.date())
            return str(obj)
        elif isinstance(obj, (date, datetime)):
            return str(obj)
        elif callable(obj):
            return "callable: " + obj.__module__ + "." + obj.__qualname__
        elif isclass(obj) and issubclass(obj, Exception):
            return obj.__name__
        else:
            return super().default(obj)

    def iterencode(self, o, _one_shot=False):
        for s in super(AuditJSONEncoder, self).iterencode(o, _one_shot=_one_shot):
            if '"@@[' in s:
                s = re.sub(r'\\"', '"', s.replace('"@@[', "["))
            if ']@@"' in s:
                s = re.sub(r'\\"', '"', s.replace(']@@"', "]"))
            yield s


def create_footings_json_file(d, file, **kwargs):
    with open(file, "w") as data_file:
        json.dump(obj=d, fp=data_file, indent=2, cls=AuditJSONEncoder, **kwargs)
