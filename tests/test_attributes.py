import inspect

from attr import attrs
from attr.setters import frozen, FrozenAttributeError
import pytest

from footings.attributes import (
    define_return,
    define_intermediate,
    define_meta,
    define_sensitivity,
    define_parameter,
    Return,
    Intermediate,
    Meta,
    Sensitivity,
    Parameter,
)


def test_attributes():
    @attrs(kw_only=True, on_setattr=frozen)
    class Test:
        asset = define_return()
        placeholder = define_intermediate()
        meta = define_meta(meta="meta")
        modifier = define_sensitivity(default=1)
        parameter = define_parameter()

    test = Test(parameter="parameter")

    # test signature
    assert inspect.getfullargspec(Test).kwonlyargs == ["modifier", "parameter"]

    # test fields
    attributes = {x.name: x for x in Test.__attrs_attrs__}
    attributes["asset"].metadata["footing_group"] is Return
    attributes["placeholder"].metadata["footing_group"] is Intermediate
    attributes["meta"].metadata["footing_group"] is Meta
    attributes["modifier"].metadata["footing_group"] is Sensitivity
    attributes["parameter"].metadata["footing_group"] is Parameter

    # test values
    assert test.parameter == "parameter"
    assert test.modifier == 1
    assert test.meta == "meta"
    assert test.asset is None
    assert test.placeholder is None

    # test frozen
    with pytest.raises(FrozenAttributeError):
        test.parameter = "change"
        test.modifier = 2
        test.meta = "change"

    test.asset = 1
    assert test.asset == 1
    test.placeholder = 2
    assert test.placeholder == 2
