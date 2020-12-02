from datetime import date, datetime
import json

import pandas as pd


def _set_key(k):
    if type(k) not in [str, int, float, bool, None]:
        return str(k)
    return k


class AuditJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, "to_audit_json"):
            return obj.to_audit_json()
        elif isinstance(obj, pd.DataFrame):
            return [{col: obj[col].to_list()} for col in obj.columns]
        elif isinstance(obj, pd.Series):
            return [{obj.name: obj.to_list()}]
        elif isinstance(obj, (pd.Timestamp, date, datetime)):
            return str(obj)
        elif callable(obj):
            return "callable: " + obj.__module__ + "." + obj.__qualname__
        else:
            try:
                return super().default(obj)
            except:
                raise TypeError(f"Object of type {type(obj)} is not serializable.")


def create_audit_json_file(audit_dict, file, **kwargs):
    with open(file, "w") as data_file:
        json.dump(obj=audit_dict, fp=data_file, cls=AuditJSONEncoder, indent=2, **kwargs)
