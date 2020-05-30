"""Test for model.py"""

# pylint: disable=missing-function-docstring, protected-access

from inspect import getfullargspec

import pytest

from footings.footing import create_footing_from_list
from footings.model import (
    build_model,
    create_dependency_index,
    create_attributes,
    ModelScenarioAlreadyExist,
    ModelScenarioDoesNotExist,
    ModelScenarioArgAlreadyExist,
    ModelScenarioArgDoesNotExist,
)

from .shared import steps_using_integers


def test_create_dependency_index():

    # c is depdent on a and b > output_src c
    dep_1 = {"a": set(), "b": set(), "c": set(["a", "b"])}
    dep_index_1 = {"a": set(["a"]), "b": set(["a", "b"]), "c": set(["c"])}
    assert create_dependency_index(dep_1) == dep_index_1

    # b is depedent on a, c is dependent on b > output_src c
    dep_2 = {"a": set(), "b": set(["a"]), "c": set(["b"])}
    dep_index_2 = {"a": set(["a"]), "b": set(["b"]), "c": set(["c"])}
    assert create_dependency_index(dep_2) == dep_index_2

    # c is dependent on a > output_src c
    dep_3 = {"a": set(), "b": set(), "c": set(["a"])}
    dep_index_3 = {"a": set(["a"]), "b": set(["a"]), "c": set(["c"])}
    assert create_dependency_index(dep_3) == dep_index_3


def test_create_attributes():
    footing = create_footing_from_list("test", steps=steps_using_integers())
    meta = {}
    attributes = create_attributes(footing, meta)
    keys = [
        "a",
        "b",
        "c",
        "arguments",
        "steps",
        "dependencies",
        "dependency_index",
        "meta",
    ]
    assert list(attributes.keys()) == keys
    assert [k for k, v in attributes.items() if v.init is True] == keys[:3]


def test_create_model_docstring():
    model = build_model(
        "model", steps=steps_using_integers(), description="This is a test",
    )
    test_doc = [
        "This is a test\n",
        "\n",
        "Arguments\n",
        "---------\n",
        "a\n",
        "\tdescription for a\n",
        "b\n",
        "\tdescription for b\n",
        "c\n",
        "\tdescription for c\n",
        "\n",
        "Returns\n",
        "-------\n",
        "int",
    ]
    print("".join(test_doc))
    print(model.__doc__)
    assert model.__doc__ == "".join(test_doc)


def test_model():

    # no output_src
    model_1 = build_model("model_1", steps=steps_using_integers())
    assert getfullargspec(model_1).kwonlyargs == ["a", "b", "c"]
    test_1 = model_1(a=1, b=1, c=1)
    assert test_1.run() == 3


def test_model_scenarios():

    model = build_model("model", steps=steps_using_integers())
    model.register_scenario("test", a=1, b=1, c=1)

    assert model._scenarios == {"test": {"a": 1, "b": 1, "c": 1}}
    assert model.using_scenario("test").run() == 3

    with pytest.raises(ModelScenarioAlreadyExist):
        model.register_scenario("test", a=1, b=1, c=1)

    with pytest.raises(ModelScenarioDoesNotExist):
        model.using_scenario("test-not-exist")

    with pytest.raises(ModelScenarioArgAlreadyExist):
        model.using_scenario("test", a=2)

    with pytest.raises(ModelScenarioArgDoesNotExist):
        model.register_scenario("test-2", not_exist_a=1, b=1, c=1)
