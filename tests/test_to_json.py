import os
import datetime
import json

from attr import attrs, attrib
import pandas as pd

from footings.to_json import create_audit_json_file


def test_footings_json(tmp_path):

    # test have been copied from recipical xlsx file
    test_dict = {}

    # test builtins
    test_builtins = {
        "test-bool": True,
        "test-str": "string",
        "test-int": 1,
        "test-float": 1.23456789,
        "test-date": datetime.date(2018, 12, 31),
        "test-datetime": datetime.datetime(2018, 12, 31, 12, 0, 0, 0),
    }
    test_dict.update({"test-builtins": test_builtins})

    # test pd.Series
    test_series = {
        "test-numbers": pd.Series([1, 2, 3], name="numbers"),
        "test-letters": pd.Series(["a", "b", "c"], name="letters"),
    }
    test_dict.update({"test-series": test_series})

    # test pd.DataFrame
    df = pd.DataFrame({"numbers": [1, 2], "letters": ["a", "b"]})
    test_dataframe = {
        "test-dataframe": df,
    }
    test_dict.update({"test-dataframe": test_dataframe})

    # test mapping
    test_mapping = {
        "test-mapping1": {"key": "value"},
        "test-mapping2": {"key": [1, 2, 3, 4]},
        "test-mapping3": {"key": df},
        # This currently fails as JSONEncoder only allows keys of int, float, str, and None
        # "test-mapping4": {("tuple", "key"): 1},
    }
    test_dict.update({"test-mapping": test_mapping})

    # test iterable
    test_iterable = {
        "test-iterable1": [1, 2, 3],
        "test-iterable2": [[1, 2, 3], [1, 2, 3]],
        "test-iterable3": [df, df],
    }
    test_dict.update({"test-iterable": test_iterable})

    # test custom
    @attrs
    class CustomOutput:
        """Custom Output"""

        a: int = attrib()
        b: int = attrib()

        def to_audit_json(self):
            return {"a": self.a, "b": self.b}

    custom1 = CustomOutput(1, 2)
    custom2 = CustomOutput(2, 4)

    test_dict.update({"test-custom": {"test-custom1": custom1, "test-custom2": custom2}})

    # test callable
    def test_func(a, b):
        return a, b

    test_dict.update({"test-function": test_func})

    test_dict_file = os.path.join(tmp_path, "test-footings-json.json")
    create_audit_json_file(test_dict, test_dict_file)
    with open(test_dict_file, "r") as file:
        test_json = json.load(file)

    expected_json_file = os.path.join("tests", "data", "expected-footings-json.json")
    with open(expected_json_file, "r") as file:
        expected_json = json.load(file)

    assert test_json == expected_json
