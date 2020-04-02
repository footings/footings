"""Test for model.py"""

# pylint: disable=missing-function-docstring

from inspect import getfullargspec
from typing import NamedTuple

import pytest

from footings.argument import Argument
from footings.footing import Use, create_footing_from_list
from footings.model import (
    build_model,
    create_dependency_index,
    create_attributes,
    ModelScenarioAlreadyExist,
    ModelScenarioDoesNotExist,
    ModelScenarioArgAlreadyExist,
    ModelScenarioArgDoesNotExist,
    register_output,
)


def steps():
    def step_1(a, add):
        """Run step_1"""
        return a + add

    def step_2(b, subtract):
        """Run step_2"""
        return b - subtract

    def step_3(a, b, c):
        """Run step_3"""
        return a + b + c

    list_ = [
        {
            "name": "step_1",
            "function": step_1,
            "args": {"arg_a": Argument("a", description="description for a"), "add": 1},
        },
        {
            "name": "step_2",
            "function": step_2,
            "args": {
                "arg_b": Argument("b", description="description for b"),
                "subtract": 1,
            },
        },
        {
            "name": "step_3",
            "function": step_3,
            "args": {
                "a": Use("step_1"),
                "b": Use("step_2"),
                "arg_c": Argument("c", description="description for c"),
            },
        },
    ]
    return list_


def test_create_dependency_index():

    # c is depdent on a and b > output_src c
    dep_1 = {"a": set(), "b": set(), "c": set(["a", "b"])}
    dep_index_1 = {"a": set(["a"]), "b": set(["a", "b"]), "c": set(["c"])}
    assert create_dependency_index(dep_1, output_src=("c",)) == dep_index_1

    # b is depedent on a, c is dependent on b > output_src c
    dep_2 = {"a": set(), "b": set(["a"]), "c": set(["b"])}
    dep_index_2 = {"a": set(["a"]), "b": set(["b"]), "c": set(["c"])}
    assert create_dependency_index(dep_2, output_src=("c",)) == dep_index_2

    # c is dependent on a > output_src c
    dep_3 = {"a": set(), "b": set(), "c": set(["a"])}
    dep_index_3 = {"a": set(["a"]), "b": set(["a"]), "c": set(["c"])}
    assert create_dependency_index(dep_3, output_src=("c",)) == dep_index_3

    # no dependencies between a, b, and c > output_src "a", "b"
    dep_4 = {"a": set(), "b": set(), "c": set()}
    dep_index_4 = {"a": set(["a"]), "b": set(["a", "b"]), "c": set(["a", "b"])}
    assert create_dependency_index(dep_4, output_src=("a", "b")) == dep_index_4

    # b dependent on a > output_src b, c
    dep_5 = {"a": set(), "b": set(["a"]), "c": set()}
    dep_index_5 = {"a": set(["a"]), "b": set(["b"]), "c": set(["b", "c"])}
    assert create_dependency_index(dep_5, output_src=("b", "c")) == dep_index_5


def test_create_attributes():
    footing = create_footing_from_list("test", steps=steps())
    output_src = ("step_3",)
    meta = {}
    attributes = create_attributes(footing, output_src, meta)
    keys = [
        "arg_a",
        "arg_b",
        "arg_c",
        "arguments",
        "output_src",
        "steps",
        "dependencies",
        "dependency_index",
        "meta",
    ]
    assert list(attributes.keys()) == keys
    assert [k for k, v in attributes.items() if v.init is True] == keys[:3]


def test_create_model_docstring():
    model = build_model(
        "model", steps=steps(), description="This is a test", output_src=("step_3",)
    )
    test_doc = [
        "This is a test\n",
        "\n",
        "Arguments\n",
        "---------\n",
        "arg_a\n",
        "\tdescription for a\n",
        "arg_b\n",
        "\tdescription for b\n",
        "arg_c\n",
        "\tdescription for c\n",
        "\n",
        "Returns\n",
        "-------\n",
        "step_3",
    ]
    print("".join(test_doc))
    print(model.__doc__)
    assert model.__doc__ == "".join(test_doc)


def test_model():

    # no output_src
    model_1 = build_model("model_1", steps=steps())
    assert getfullargspec(model_1).kwonlyargs == ["arg_a", "arg_b", "arg_c"]
    test_1 = model_1(arg_a=1, arg_b=1, arg_c=1)
    assert test_1.run() == 3

    # defined output_src
    model_2 = build_model("model_2", steps=steps(), output_src=("step_3",))
    assert getfullargspec(model_2).kwonlyargs == ["arg_a", "arg_b", "arg_c"]
    test_2 = model_2(arg_a=1, arg_b=1, arg_c=1)
    assert test_2.run() == 3

    # defined output_src multiple
    output_src = ("step_1", "step_2", "step_3")
    model_3 = build_model("model_3", steps=steps(), output_src=output_src)
    assert getfullargspec(model_3).kwonlyargs == ["arg_a", "arg_b", "arg_c"]
    test_3 = model_3(arg_a=1, arg_b=1, arg_c=1)
    assert test_3.run() == (2, 0, 3)

    # defined output_src multiple using NamedTuple
    class OutputType(NamedTuple):
        """OutputType"""

        step_1: int
        step_2: int
        step_3: int

    register_output(
        OutputType,
        lambda output_src: set(output_src._fields),
        lambda output_src, dict_: output_src(**dict_),
    )

    model_4 = build_model("model_4", steps=steps(), output_src=OutputType)
    assert getfullargspec(model_4).kwonlyargs == ["arg_a", "arg_b", "arg_c"]
    test_4 = model_3(arg_a=1, arg_b=1, arg_c=1)
    assert test_4.run() == OutputType(step_1=2, step_2=0, step_3=3)


def test_model_scenarios():

    model = build_model("model", steps=steps(), output_src=("step_3",))
    model.register_scenario("test", arg_a=1, arg_b=1, arg_c=1)
    assert model._scenarios == {"test": {"arg_a": 1, "arg_b": 1, "arg_c": 1}}
    assert model.using_scenario("test").run() == 3

    with pytest.raises(ModelScenarioAlreadyExist):
        model.register_scenario("test", arg_a=1, arg_b=1, arg_c=1)

    with pytest.raises(ModelScenarioDoesNotExist):
        model.using_scenario("test-not-exist")

    with pytest.raises(ModelScenarioArgAlreadyExist):
        model.using_scenario("test", arg_a=2)

    with pytest.raises(ModelScenarioArgDoesNotExist):
        model.register_scenario("test-2", not_exist_a=1, arg_b=1, arg_c=1)