import pytest

from footings.core.utils import (
    DispatchFunction,
    DispatchFunctionKeyError,
    DispatchFunctionRegisterParameterError,
    LoadedFunction,
)


def test_dispatch_default():
    def _default():
        return "default"

    test = DispatchFunction(name="test", parameters=("key",), default=_default)

    @test.register(key="x")
    def _():
        return "x"

    @test.register(key="y")
    def _():
        return "y"

    assert test(key="x") == "x"
    assert test(key="y") == "y"
    assert test(key="z") == "default"


def test_dispatch_one_parameter_single_value():
    docstring = """Test dispatch function."""
    test = DispatchFunction(name="test", parameters=("key",), docstring=docstring)

    @test.register(key="x")
    def _():
        return "x"

    @test.register(key="y")
    def _():
        return "y"

    assert test(key="x") == "x"
    assert test(key="y") == "y"
    pytest.raises(DispatchFunctionKeyError, test, key="z")

    expected_doc = [
        "Test dispatch function.",
        "",
        "Parameters",
        "----------",
        "key",
        "",
        "Dispatch",
        "--------",
        "key = x",
        "\t()",
        "key = y",
        "\t()",
        "",
    ]
    assert test.__doc__ == "\n".join(expected_doc)


def test_dispatch_one_parameter_multiple_values():
    test = DispatchFunction(name="test", parameters=("key",))

    @test.register(key=["x1", "x2", "x3"])
    def _(arg):
        return arg

    @test.register(key="y")
    def _(arg):
        return arg

    assert test(key="x1", arg=1) == 1
    assert test(key="x2", arg=1) == 1
    assert test(key="x3", arg=1) == 1
    assert test(key="y", arg=1) == 1
    pytest.raises(DispatchFunctionKeyError, test, key="z")

    expected_doc = [
        "Parameters",
        "----------",
        "key",
        "",
        "Dispatch",
        "--------",
        "key = x1",
        "\t(arg)",
        "key = x2",
        "\t(arg)",
        "key = x3",
        "\t(arg)",
        "key = y",
        "\t(arg)",
        "",
    ]
    assert test.__doc__ == "\n".join(expected_doc)


def test_dispatch_multiple_parameters_single_value():
    test = DispatchFunction(name="test", parameters=("key1", "key2"))

    @test.register(key1="x1", key2="x2")
    def _():
        return "x"

    @test.register(key1="y1", key2="y2")
    def _():
        return "y"

    assert test(key1="x1", key2="x2") == "x"
    assert test(key1="y1", key2="y2") == "y"
    pytest.raises(DispatchFunctionKeyError, test, key1="x", key2="z")

    expected_doc = [
        "Parameters",
        "----------",
        "key1",
        "key2",
        "",
        "Dispatch",
        "--------",
        "key1 = x1, key2 = x2",
        "\t()",
        "key1 = y1, key2 = y2",
        "\t()",
        "",
    ]
    assert test.__doc__ == "\n".join(expected_doc)


def test_dispatch_many_parameter_multiple_values():
    test = DispatchFunction(name="test", parameters=("key1", "key2", "key3"))

    @test.register(key1=["x1", "x2", "x3"], key2=["xa", "xb"], key3="xz")
    def _():
        return "x"

    @test.register(key1=["y1", "y2"], key2=["ya"], key3="y")
    def _():
        return "y"

    assert test(key1="x1", key2="xa", key3="xz") == "x"
    assert test(key1="x2", key2="xa", key3="xz") == "x"
    assert test(key1="x3", key2="xa", key3="xz") == "x"
    assert test(key1="x1", key2="xb", key3="xz") == "x"
    assert test(key1="x2", key2="xb", key3="xz") == "x"
    assert test(key1="x3", key2="xb", key3="xz") == "x"
    assert test(key1="y1", key2="ya", key3="y") == "y"
    assert test(key1="y2", key2="ya", key3="y") == "y"
    pytest.raises(DispatchFunctionKeyError, test, key1="x1", key2="xa", key3="z")

    expected_doc = [
        "Parameters",
        "----------",
        "key1",
        "key2",
        "key3",
        "",
        "Dispatch",
        "--------",
        "key1 = x1, key2 = xa, key3 = xz",
        "\t()",
        "key1 = x1, key2 = xb, key3 = xz",
        "\t()",
        "key1 = x2, key2 = xa, key3 = xz",
        "\t()",
        "key1 = x2, key2 = xb, key3 = xz",
        "\t()",
        "key1 = x3, key2 = xa, key3 = xz",
        "\t()",
        "key1 = x3, key2 = xb, key3 = xz",
        "\t()",
        "key1 = y1, key2 = ya, key3 = y",
        "\t()",
        "key1 = y2, key2 = ya, key3 = y",
        "\t()",
        "",
    ]
    assert test.__doc__ == "\n".join(expected_doc)


def test_dispatch_raise_errors():
    test = DispatchFunction(name="test", parameters=("key",))

    with pytest.raises(DispatchFunctionRegisterParameterError):

        @test.register(wrong_key="x")
        def _():
            return "x"


def test_loaded_function():
    def main_func(a, b):
        """Main function"""
        return a + b

    test_func = LoadedFunction("test_func", function=main_func)
    print(test_func.function.__doc__)

    @test_func.register(position="start")
    def pre_hook(a, b):
        b += 1
        return a, b

    @test_func.register
    def post_hook_1(x):
        return x + 1

    assert test_func(a=1, b=1) == 4
    expected = [
        "Main function",
        "",
        "Loaded",
        "------",
        "pre_hook",
        "main_func",
        "post_hook_1",
        "",
    ]
    assert test_func.__doc__ == "\n".join(expected)
