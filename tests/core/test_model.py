from inspect import getfullargspec

import pytest
import pandas as pd
from pandas.testing import assert_series_equal

from footings.core.footing import create_footing_from_list
from footings.core.model import (
    create_model,
    create_dependency_index,
    create_attributes,
    ModelScenarioAlreadyExist,
    ModelScenarioDoesNotExist,
    ModelScenarioArgAlreadyExist,
    ModelScenarioArgDoesNotExist,
    ModelRunError,
)

from .shared import STEPS_USING_INTEGERS, STEPS_USING_ATTR_LOOKUP, STEPS_USING_KEY_LOOKUP


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
    footing = create_footing_from_list("test", steps=STEPS_USING_INTEGERS)
    attributes = create_attributes(footing)
    keys = [
        "a",
        "b",
        "c",
        "arguments",
        "steps",
        "dependencies",
        "dependency_index",
    ]
    assert list(attributes.keys()) == keys
    assert [k for k, v in attributes.items() if v.init is True] == keys[:3]


def test_create_model_docstring():
    model = create_model(
        "model", steps=STEPS_USING_INTEGERS, description="This is a test",
    )
    test_doc = [
        "This is a test\n",
        "\n",
        "Parameters\n",
        "----------\n",
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
    assert model.__doc__ == "".join(test_doc)


def test_model():

    model_1 = create_model("model_1", steps=STEPS_USING_INTEGERS)
    assert getfullargspec(model_1).kwonlyargs == ["a", "b", "c"]
    test_1 = model_1(a=1, b=1, c=1)
    assert test_1.run() == 3

    model_2 = create_model("model_2", steps=STEPS_USING_KEY_LOOKUP)
    assert getfullargspec(model_2).kwonlyargs == ["a", "b"]
    test_2 = model_2(a="a", b="b")
    assert test_2.run() == "a"

    model_3 = create_model("model_3", steps=STEPS_USING_ATTR_LOOKUP)
    assert getfullargspec(model_3).kwonlyargs == ["n", "add", "subtract"]
    test_3 = model_3(n=3, add=1, subtract=2)
    assert_series_equal(test_3.run(), pd.Series([0, 1, 2], name="n"))


def test_model_errors():
    model_1 = create_model("model_1", steps=STEPS_USING_INTEGERS)
    test_1 = model_1(a=1, b=1, c="c")
    with pytest.raises(ModelRunError):
        test_1.run()


def test_model_scenarios():

    model = create_model("model", steps=STEPS_USING_INTEGERS)
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
