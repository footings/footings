"""test for footing.py"""

# pylint: disable=missing-function-docstring

from footings.argument import Argument
from footings.footing import (
    Footing,
    FootingStep,
    create_footing_from_list,
    Dependent,
)


def test_footing():
    test = Footing("test")

    def step_1(a, add):
        return a + add

    test.add_step(name="step_1", function=step_1, args={"arg_a": Argument("a"), "add": 1})

    def step_2(b, subtract):
        return b - subtract

    test.add_step(
        name="step_2", function=step_2, args={"arg_b": Argument("b"), "subtract": 1}
    )

    def step_3(a, b, c):
        return a, b, c

    test.add_step(
        name="step_3",
        function=step_3,
        args={
            "a": Dependent("step_1"),
            "b": Dependent("step_2"),
            "arg_c": Argument("c"),
        },
    )

    assert test.dependencies == {
        "step_1": set(),
        "step_2": set(),
        "step_3": set(["step_1", "step_2"]),
    }
    assert test.arguments == {
        "a": Argument("a"),
        "b": Argument("b"),
        "c": Argument("c"),
    }
    assert test.steps == {
        "step_1": FootingStep(
            function=step_1,
            init_args={"arg_a": "a"},
            defined_args={"add": 1},
            dependent_args={},
        ),
        "step_2": FootingStep(
            function=step_2,
            init_args={"arg_b": "b"},
            defined_args={"subtract": 1},
            dependent_args={},
        ),
        "step_3": FootingStep(
            function=step_3,
            init_args={"arg_c": "c"},
            defined_args={},
            dependent_args={"a": "step_1", "b": "step_2"},
        ),
    }


def test_create_footing_from_list():
    def step_1(a, add):
        return a + add

    def step_2(b, subtract):
        return b - subtract

    def step_3(a, b, c):
        return a, b, c

    steps = [
        {
            "name": "step_1",
            "function": step_1,
            "args": {"arg_a": Argument("a"), "add": 1},
        },
        {
            "name": "step_2",
            "function": step_2,
            "args": {"arg_b": Argument("b"), "subtract": 1},
        },
        {
            "name": "step_3",
            "function": step_3,
            "args": {
                "a": Dependent("step_1"),
                "b": Dependent("step_2"),
                "arg_c": Argument("c"),
            },
        },
    ]
    test = create_footing_from_list("test", steps)
    assert test.dependencies == {
        "step_1": set(),
        "step_2": set(),
        "step_3": set(["step_1", "step_2"]),
    }
    assert test.arguments == {
        "a": Argument("a"),
        "b": Argument("b"),
        "c": Argument("c"),
    }
    assert test.steps == {
        "step_1": FootingStep(
            function=step_1,
            init_args={"arg_a": "a"},
            defined_args={"add": 1},
            dependent_args={},
        ),
        "step_2": FootingStep(
            function=step_2,
            init_args={"arg_b": "b"},
            defined_args={"subtract": 1},
            dependent_args={},
        ),
        "step_3": FootingStep(
            function=step_3,
            init_args={"arg_c": "c"},
            defined_args={},
            dependent_args={"a": "step_1", "b": "step_2"},
        ),
    }
