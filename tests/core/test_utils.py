import pytest

from footings.core.utils import (
    dispatch_function,
    DispatchMissingArgumentError,
    loaded_function,
)


def test_dispatch_function_single_key():
    @dispatch_function(key_parameters=("key",))
    def dispatch(key):
        return "default"

    @dispatch.register(key="x")
    def _():
        return "x"

    @dispatch.register(key="y")
    def _():
        return "y"

    assert dispatch(key="x") == "x"
    assert dispatch(key="y") == "y"
    assert dispatch(key="z") == "default"
    assert dispatch(key=None) == "default"


def test_dispatch_one_parameter_multiple_values():
    @dispatch_function(key_parameters=("key",))
    def dispatch(key, arg):
        raise NotImplementedError()

    @dispatch.register(key=["x1", "x2", "x3"])
    def _(arg):
        return arg

    @dispatch.register(key="y")
    def _(arg):
        return arg

    assert dispatch(key="x1", arg=1) == 1
    assert dispatch(key="x2", arg=1) == 1
    assert dispatch(key="x3", arg=1) == 1
    assert dispatch(key="y", arg=1) == 1
    pytest.raises(NotImplementedError, dispatch, key="z", arg=1)


def test_dispatch_multiple_parameters_single_value():
    @dispatch_function(key_parameters=("key1", "key2"))
    def dispatch(key1, key2):
        raise NotImplementedError()

    @dispatch.register(key1="x1", key2="x2")
    def _():
        return "x"

    @dispatch.register(key1="y1", key2="y2")
    def _():
        return "y"

    assert dispatch(key1="x1", key2="x2") == "x"
    assert dispatch(key1="y1", key2="y2") == "y"
    pytest.raises(NotImplementedError, dispatch, key1="x", key2="z")


def test_dispatch_many_parameter_multiple_values():
    @dispatch_function(key_parameters=("key1", "key2", "key3"))
    def dispatch(key1, key2, key3):
        raise NotImplementedError()

    @dispatch.register(key1=["x1", "x2", "x3"], key2=["xa", "xb"], key3="xz")
    def _():
        return "x"

    @dispatch.register(key1=["y1", "y2"], key2=["ya"], key3="y")
    def _():
        return "y"

    assert dispatch(key1="x1", key2="xa", key3="xz") == "x"
    assert dispatch(key1="x2", key2="xa", key3="xz") == "x"
    assert dispatch(key1="x3", key2="xa", key3="xz") == "x"
    assert dispatch(key1="x1", key2="xb", key3="xz") == "x"
    assert dispatch(key1="x2", key2="xb", key3="xz") == "x"
    assert dispatch(key1="x3", key2="xb", key3="xz") == "x"
    assert dispatch(key1="y1", key2="ya", key3="y") == "y"
    assert dispatch(key1="y2", key2="ya", key3="y") == "y"
    pytest.raises(NotImplementedError, dispatch, key1="x1", key2="xa", key3="z")


def test_dispatch_missing_argument():
    @dispatch_function(key_parameters=("key1", "key2", "key3"))
    def dispatch(key1, key2, key3):
        raise NotImplementedError()

    with pytest.raises(DispatchMissingArgumentError):

        @dispatch.register(key="x")
        def _():
            return "x"


def test_loaded_function_single():
    @loaded_function
    def loaded(a, b):
        """Main function"""
        return a + b

    @loaded.register(position="start")
    def _(a, b):
        b += 1
        return a, b

    @loaded.register(position="end")
    def _(x):
        return x + 1

    with pytest.raises(ValueError):

        @loaded.register(position="zzz")
        def _(x):
            pass

    assert loaded(a=1, b=1) == 4
    assert loaded(1, 1) == 4


def test_loaded_function_existing():
    @loaded_function
    def loaded(a, b):
        """Main function"""
        return a + b

    @loaded.register(position="start")
    def _(a, b):
        b += 1
        return a, b

    @loaded.register(position="end")
    def _(x):
        return x + 1

    loaded2 = loaded_function(loaded)

    @loaded2.register(position="end")
    def _(x):
        return x + 1

    assert loaded2(a=1, b=1) == 5
    assert loaded2(1, 1) == 5
