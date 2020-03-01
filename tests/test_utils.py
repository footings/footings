"""test for utils.py"""

# pylint: disable=function-redefined, missing-function-docstring

import pytest

from footings.utils import Dispatcher, DispatcherKeyError


def test_dispatch_one_key_no_default():
    test = Dispatcher(name="test")

    @test.register("x")
    def _():
        return "x"

    @test.register("y")
    def _():
        return "y"

    assert test("x") == "x"
    assert test("y") == "y"
    pytest.raises(DispatcherKeyError, test, "z")


def test_dispatch_many_keys_no_default():
    test = Dispatcher(name="test")

    @test.register(["x1", "x2", "x3"])
    def _():
        return "x"

    @test.register("y")
    def _():
        return "y"

    assert test("x1") == "x"
    assert test("x2") == "x"
    assert test("x3") == "x"
    assert test("y") == "y"
    pytest.raises(DispatcherKeyError, test, "z")


def test_dispatch_one_key_with_default():
    def _default():
        return "default"

    test = Dispatcher(name="test", default=_default)

    @test.register("x1")
    def _():
        return "x"

    @test.register("y")
    def _():
        return "y"

    assert test("x1") == "x"
    assert test("y") == "y"
    assert test("z") == "default"


def test_dispatch_many_keys_with_default():
    def _default():
        return "default"

    test = Dispatcher(name="test", default=_default)

    @test.register(["x1", "x2", "x3"])
    def _():
        return "x"

    @test.register("y")
    def _():
        return "y"

    assert test("x1") == "x"
    assert test("x2") == "x"
    assert test("x3") == "x"
    assert test("y") == "y"
    assert test("z") == "default"
