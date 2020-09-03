from attr import attrib
from numpydoc.docscrape import Parameter
import pytest

from footings.core.attributes import (
    define_asset,
    define_meta,
    define_modifier,
    define_parameter,
)
from footings.core.model import Footing, FootingsDoc, model, step


def test_model_instantiation():

    with pytest.raises(TypeError):

        @model(steps=[])
        class MissingFooting:
            x = 1

    with pytest.raises(AttributeError):

        # fails due to using a value vs using one of define_[assets, meta, modifier, parameter]
        @model(steps=["add"])
        class FailUsingValue(Footing):
            x = 1

        # fails due to using attriv() vs using one of define_[assets, meta, modifier, parameter]
        @model(steps=["add"])
        class FailUsingAttrib(Footing):
            x = attrib()

        # fail due to missing at least one attribute defined using define_asset()
        @model(steps=["add"])
        class FailMissingAsset:
            x = define_parameter()

        # fail due to missing step as method
        @model(steps=["add"])
        class FailMissingStep:
            x = define_parameter()
            y = define_asset()


def test_model_documentation():
    @model(steps=["_add", "_subtract"])
    class Test(Footing):
        asset = define_asset(dtype="int", description="This is an asset.")
        meta = define_meta(meta="meta", description="This is meta.")
        modifier = define_modifier(default=1, description="This is a modifier.")
        parameter = define_parameter(description="This is a parameter.")

        def _add(self):
            """Do addition."""
            pass

        def _subtract(self):
            """Do subtraction."""
            pass

    doc = FootingsDoc(Test)
    doc["Assets"] = [Parameter("asset", "int", ["This is an asset."])]
    doc["Meta"] = [Parameter("meta", None, ["This is meta."])]
    doc["Modifiers"] = [Parameter("modifier", None, ["this is a modifier."])]
    doc["Parameters"] = [Parameter("parameter", None, ["This is a parameter."])]


def test_model_steps():
    @model(steps=["_add", "_subtract"])
    class Test(Footing):
        x = define_parameter()
        y = define_parameter()
        z = define_parameter()
        out = define_asset()

        @step(uses=["x", "y"], impacts=["out"])
        def _add(self):
            self.out = self.x + self.y

        @step(uses=["z", "out"], impacts=["out"])
        def _subtract(self):
            self.out = self.out - self.z

    test = Test(x=1, y=2, z=3)
    assert test.run() == 0
