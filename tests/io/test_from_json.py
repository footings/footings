import os
from footings.io import load_footings_file
from footings.io.from_json import flatten_dict, load_footings_json_file


test_dict = {"outer": {"inner": {"endpoint1": 1, "endpoint2": 2}}, "endpoint3": 3}
expected_dict = {
    "/outer/inner/endpoint1": 1,
    "/outer/inner/endpoint2": 2,
    "/endpoint3": 3,
}


def test_flatten_dict():
    assert flatten_dict(test_dict) == expected_dict


def test_load_json_file():
    # from footings.io.to_json import create_footings_json_file
    # create_footings_json_file(test_dict, "test-load-json-file.json")
    test_file = os.path.join("tests", "io", "data", "expected-load-file.json")
    assert load_footings_file(test_file) == expected_dict
    assert load_footings_json_file(test_file) == expected_dict
