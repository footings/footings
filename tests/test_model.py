from attr import attrib
from numpydoc.docscrape import Parameter
import pytest

from footings.attributes import (
    define_asset,
    define_meta,
    define_modifier,
    define_parameter,
    define_placeholder,
)

from footings.model import Footing, FootingsDoc, model, step, ModelCreationError


def test_model_instantiation():

    with pytest.raises(ModelCreationError):

        # fails due to not subclass of Footing
        @model(steps=["_add"])  # noqa: F841
        class MissingFooting:
            parameter = define_parameter(dtype=int)
            asset = define_asset(default=0)

            @step(uses=["parameter"], impacts=["assets"])
            def _add(self):
                self.asset = self.asset + self.parameter

        # fails due to using a value vs using one of define_[assets, meta, modifier, parameter]
        @model(steps=["_add"])
        class FailUsingValue(Footing):
            parameter = define_parameter(dtype=int)
            asset = 0

            @step(uses=["parameter"], impacts=["assets"])
            def _add(self):
                self.asset = self.asset + self.parameter

        # fails due to using attrib() vs using one of define_[assets, meta, modifier, parameter]
        @model(steps=["_add"])
        class FailUsingAttrib(Footing):
            parameter = define_parameter(dtype=int)
            asset = attrib(default=0)

            @step(uses=["parameter"], impacts=["assets"])
            def _add(self):
                self.asset = self.asset + self.parameter

        # fail due to missing at least one attribute defined using define_asset()
        @model(steps=["_add"])
        class FailMissingAsset:
            parameter = define_parameter(dtype=int)
            asset = define_parameter(default=0)

            @step(uses=["parameter"], impacts=["assets"])
            def _add(self):
                self.asset = self.asset + self.parameter

        # fail due to missing step as method
        @model(steps=[])
        class FailZeroSteps(Footing):
            x = define_parameter()
            y = define_asset()

        # fail due to missing step as method
        @model(steps=["_add"])
        class FailMissingStep(Footing):
            parameter = define_parameter(dtype=int)
            asset = define_asset(default=0)

        # fail due to step not decorated
        @model(steps=["_add"])  # noqa: F841
        class FailStepNotDecorated(Footing):
            parameter = define_parameter(dtype=int)
            asset = define_asset(default=0)

            def _add(self):
                self.asset = self.asset + self.parameter


def test_model_documentation():
    @model(steps=["_add", "_subtract"])
    class Test(Footing):
        asset = define_asset(dtype="int", description="This is an asset.")
        meta = define_meta(meta="meta", description="This is meta.")
        modifier = define_modifier(default=1, description="This is a modifier.")
        pmeter = define_parameter(description="This is a parameter.")

        @step(uses=["pmeter"], impacts=["asset"])
        def _add(self):
            """Do addition."""
            pass

        @step(uses=["asset", "modifier"], impacts=["asset"])
        def _subtract(self):
            """Do subtraction."""
            pass

    doc = FootingsDoc(Test)
    doc["Assets"] = [Parameter("asset", "int", ["This is an asset."])]
    doc["Meta"] = [Parameter("meta", None, ["This is meta."])]
    doc["Modifiers"] = [Parameter("modifier", None, ["this is a modifier."])]
    doc["Parameters"] = [Parameter("parameter", None, ["This is a parameter."])]


def test_model_attributes():
    @model(steps=["_step1"])
    class TestAttributes(Footing):
        param1 = define_parameter()
        param2 = define_parameter()
        modifier1 = define_modifier(default=1)
        modifier2 = define_modifier(default=2)
        meta1 = define_meta(meta="meta1", dtype=str)
        meta2 = define_meta(meta="meta2", dtype=str)
        placeholder1 = define_placeholder()
        placeholder2 = define_placeholder()
        asset1 = define_asset()
        asset2 = define_asset()

        @step(uses=["param1", "param2"], impacts=["asset1"])
        def _step1(self):
            pass

    # instanticate model
    init_model = TestAttributes(param1=1, param2=2)

    # test params
    params = {"param1": "parameter.param1", "param2": "parameter.param2"}
    assert init_model.__footings_parameters__ == tuple(params.keys())
    modifiers = {"modifier1": "modifier.modifier1", "modifier2": "modifier.modifier2"}
    assert init_model.__footings_modifiers__ == tuple(modifiers.keys())
    meta = {"meta1": "meta.meta1", "meta2": "meta.meta2"}
    assert init_model.__footings_meta__ == tuple(meta.keys())
    placeholders = {
        "placeholder1": "placeholder.placeholder1",
        "placeholder2": "placeholder.placeholder2",
    }
    assert init_model.__footings_placeholders__ == tuple(placeholders.keys())
    assets = {"asset1": "asset.asset1", "asset2": "asset.asset2"}
    assert init_model.__footings_assets__ == tuple(assets.keys())
    assert init_model.__footings_attribute_map__ == {
        **params,
        **modifiers,
        **meta,
        **placeholders,
        **assets,
    }


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
