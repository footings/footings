import inspect

from attr import attrs
from attr.setters import frozen, FrozenAttributeError
import pytest

from footings.attributes import (
    def_return,
    def_intermediate,
    def_meta,
    def_sensitivity,
    def_parameter,
    FootingsAttributeType,
)


def test_attributes():
    @attrs(kw_only=True, on_setattr=frozen)
    class Test:
        ret = def_return()
        placeholder = def_intermediate()
        meta = def_meta(meta="meta")
        modifier = def_sensitivity(default=1)
        parameter = def_parameter()

    test = Test(parameter="parameter")

    # test signature
    assert inspect.getfullargspec(Test).kwonlyargs == ["modifier", "parameter"]

    # test fields
    def _get_type(attribute):
        return attribute.metadata["footings_attribute_type"]

    attributes = {x.name: x for x in Test.__attrs_attrs__}
    _get_type(attributes["ret"]) is FootingsAttributeType.Return
    _get_type(attributes["placeholder"]) is FootingsAttributeType.Intermediate
    _get_type(attributes["meta"]) is FootingsAttributeType.Meta
    _get_type(attributes["modifier"]) is FootingsAttributeType.Sensitivity
    _get_type(attributes["parameter"]) is FootingsAttributeType.Parameter

    # test values
    assert test.parameter == "parameter"
    assert test.modifier == 1
    assert test.meta == "meta"
    assert test.ret is None
    assert test.placeholder is None

    # test frozen
    with pytest.raises(FrozenAttributeError):
        test.parameter = "change"
        test.modifier = 2
        test.meta = "change"

    test.ret = 1
    assert test.ret == 1
    test.placeholder = 2
    assert test.placeholder == 2
