import inspect

from attr import attrs
from attr.setters import frozen, FrozenAttributeError
import pytest

from footings.core.attributes import (
    define_asset,
    define_meta,
    define_modifier,
    define_parameter,
)


def test_attributes():
    @attrs(kw_only=True, on_setattr=frozen)
    class Test:
        asset = define_asset()
        meta = define_meta(meta="meta")
        modifier = define_modifier(default=1)
        parameter = define_parameter()

    test = Test(parameter="parameter")

    # test signature
    assert inspect.getfullargspec(Test).kwonlyargs == ["modifier", "parameter"]

    # test fields
    attributes = {x.name: x for x in Test.__attrs_attrs__}
    attributes["asset"].metadata["footing_group"] == "asset"
    attributes["meta"].metadata["footing_group"] == "meta"
    attributes["modifier"].metadata["footing_group"] == "modifier"
    attributes["parameter"].metadata["footing_group"] == "parameter"

    # test values
    assert test.parameter == "parameter"
    assert test.modifier == 1
    assert test.meta == "meta"
    assert test.asset is None

    # test frozen
    with pytest.raises(FrozenAttributeError):
        test.parameter = "change"
        test.modifier = 2
        test.meta = "change"

    test.asset = 1
    assert test.asset == 1
