import pandas as pd

from footings.model_tools import convert_to_records


def test_convert_to_records():
    df = pd.DataFrame({"X": [1, 2], "y": [2, 4]})
    assert convert_to_records(df) == [{"X": 1, "y": 2}, {"X": 2, "y": 4}]
    assert convert_to_records(df, "lower") == [{"x": 1, "y": 2}, {"x": 2, "y": 4}]
    assert convert_to_records(df, "upper") == [{"X": 1, "Y": 2}, {"X": 2, "Y": 4}]
