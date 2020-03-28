"""test for footing.py"""

from footings.argument import Argument
from footings.footing import Footing, FootingStep, footing_from_list, Use


def test_footing():
    def step_1(a, add):
        return a + add

    def step_2(b, subtract):
        return b - subtract

    def step_3(a, b, c):
        return a, b, c

    steps = [
        {
            "name": "step-1",
            "function": step_1,
            "args": {"arg_a": Argument("a"), "add": 1},
        },
        {
            "name": "step-2",
            "function": step_2,
            "args": {"arg_b": Argument("b"), "subtract": 1},
        },
        {
            "name": "step-3",
            "function": step_3,
            "args": {"a": Use("step-1"), "b": Use("step-2"), "arg_c": Argument("c")},
        },
    ]
    test = footing_from_list("test", steps)
    assert test.dependencies == {
        "step-1": set(),
        "step-2": set(),
        "step-3": set(["step-1", "step-2"]),
    }
    assert test.arguments == {
        "arg_a": Argument("a"),
        "arg_b": Argument("b"),
        "arg_c": Argument("c"),
    }
    assert test.steps == {
        "step-1": FootingStep(
            function=step_1,
            init_args={"arg_a": "a"},
            defined_args={"add": 1},
            dependent_args={},
            meta={},
        ),
        "step-2": FootingStep(
            function=step_2,
            init_args={"arg_b": "b"},
            defined_args={"subtract": 1},
            dependent_args={},
            meta={},
        ),
        "step-3": FootingStep(
            function=step_3,
            init_args={"arg_c": "c"},
            defined_args={},
            dependent_args={"a": "step-1", "b": "step-2"},
            meta={},
        ),
    }
