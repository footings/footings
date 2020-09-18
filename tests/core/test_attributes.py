import inspect

from attr import attrs
from attr.setters import frozen, FrozenAttributeError
import pytest

from footings.core.attributes import (
    define_asset,
    define_placeholder,
    define_meta,
    define_modifier,
    define_parameter,
    _Asset,
    _Placeholder,
    _Meta,
    _Modifier,
    _Parameter,
)


def test_attributes():
    @attrs(kw_only=True, on_setattr=frozen)
    class Test:
        asset = define_asset()
        placeholder = define_placeholder()
        meta = define_meta(meta="meta")
        modifier = define_modifier(default=1)
        parameter = define_parameter()

    test = Test(parameter="parameter")

    # test signature
    assert inspect.getfullargspec(Test).kwonlyargs == ["modifier", "parameter"]

    # test fields
    attributes = {x.name: x for x in Test.__attrs_attrs__}
    attributes["asset"].metadata["footing_group"] is _Asset
    attributes["placeholder"].metadata["footing_group"] is _Placeholder
    attributes["meta"].metadata["footing_group"] is _Meta
    attributes["modifier"].metadata["footing_group"] is _Modifier
    attributes["parameter"].metadata["footing_group"] is _Parameter

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
