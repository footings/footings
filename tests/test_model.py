from attr import attrib
from numpydoc.docscrape import Parameter
import pytest

from footings.attributes import (
    def_return,
    def_meta,
    def_sensitivity,
    def_parameter,
    def_intermediate,
)

from footings.model import FootingsDoc, model, step, ModelCreationError


def test_model_instantiation():

    with pytest.raises(ModelCreationError):

        # fails due to not subclass of Footing
        @model(steps=["_add"])  # noqa: F841
        class MissingFooting:
            parameter = def_parameter(dtype=int)
            ret = def_return(default=0)

            @step(uses=["parameter"], impacts=["ret"])
            def _add(self):
                self.ret = self.ret + self.parameter

        # fails due to using a value vs using one of def_[return, meta, sensitivity, parameter]
        @model(steps=["_add"])
        class FailUsingValue:
            parameter = def_parameter(dtype=int)
            ret = 0

            @step(uses=["parameter"], impacts=["ret"])
            def _add(self):
                self.ret = self.ret + self.parameter

        # fails due to using attrib() vs using one of def_[return, meta, sensitivity, parameter]
        @model(steps=["_add"])
        class FailUsingAttrib:
            parameter = def_parameter(dtype=int)
            ret = attrib(default=0)

            @step(uses=["parameter"], impacts=["ret"])
            def _add(self):
                self.ret = self.ret + self.parameter

        # fail due to missing at least one attribute defined using def_return()
        @model(steps=["_add"])
        class FailMissingReturn:
            parameter = def_parameter(dtype=int)
            ret = def_parameter(default=0)

            @step(uses=["parameter"], impacts=["ret"])
            def _add(self):
                self.ret = self.ret + self.parameter

        # fail due to missing step as method
        @model(steps=[])
        class FailZeroSteps:
            x = def_parameter()
            y = def_return()

        # fail due to missing step as method
        @model(steps=["_add"])
        class FailMissingStep:
            parameter = def_parameter(dtype=int)
            ret = def_return(default=0)

        # fail due to step not decorated
        @model(steps=["_add"])  # noqa: F841
        class FailStepNotDecorated:
            parameter = def_parameter(dtype=int)
            ret = def_return(default=0)

            def _add(self):
                self.ret = self.ret + self.parameter


def test_model_documentation():
    @model(steps=["_add", "_subtract"])
    class Test:
        parameter = def_parameter(description="This is a parameter.")
        sensitivity = def_sensitivity(default=1, description="This is a sensitivity.")
        meta = def_meta(meta="meta", description="This is meta.")
        ret = def_return(dtype="int", description="This is a return.")

        @step(uses=["parameter"], impacts=["ret"])
        def _add(self):
            """Do addition."""
            pass

        @step(uses=["ret", "sensitivity"], impacts=["ret"])
        def _subtract(self):
            """Do subtraction."""
            pass

    doc = FootingsDoc(Test)
    assert doc["Returns"] == [Parameter("ret", "int", ["This is a return."])]
    assert doc["Meta"] == [Parameter("meta", "", ["This is meta."])]
    assert doc["Sensitivities"] == [
        Parameter("sensitivity", "", ["This is a sensitivity."])
    ]
    assert doc["Parameters"] == [Parameter("parameter", "", ["This is a parameter."])]


def test_model_attributes():
    @model(steps=["_step1"])
    class TestAttributes:
        param1 = def_parameter()
        param2 = def_parameter()
        sensitivity1 = def_sensitivity(default=1)
        sensitivity2 = def_sensitivity(default=2)
        meta1 = def_meta(meta="meta1", dtype=str)
        meta2 = def_meta(meta="meta2", dtype=str)
        intermediate1 = def_intermediate()
        intermediate2 = def_intermediate()
        return1 = def_return()
        return2 = def_return()

        @step(uses=["param1", "param2"], impacts=["return1"])
        def _step1(self):
            pass

    # instanticate model
    init_model = TestAttributes(param1=1, param2=2)

    # test params
    params = {"param1": "parameter.param1", "param2": "parameter.param2"}
    assert init_model.__footings_parameters__ == tuple(params.keys())
    sensitivities = {
        "sensitivity1": "sensitivity.sensitivity1",
        "sensitivity2": "sensitivity.sensitivity2",
    }
    assert init_model.__footings_sensitivities__ == tuple(sensitivities.keys())
    meta = {"meta1": "meta.meta1", "meta2": "meta.meta2"}
    assert init_model.__footings_meta__ == tuple(meta.keys())
    intermediates = {
        "intermediate1": "intermediate.intermediate1",
        "intermediate2": "intermediate.intermediate2",
    }
    assert init_model.__footings_intermediates__ == tuple(intermediates.keys())
    returns = {"return1": "return.return1", "return2": "return.return2"}
    assert init_model.__footings_returns__ == tuple(returns.keys())
    assert init_model.__footings_attribute_map__ == {
        **params,
        **sensitivities,
        **meta,
        **intermediates,
        **returns,
    }


def test_model_steps():
    @model(steps=["_add", "_subtract"])
    class Test:
        x = def_parameter()
        y = def_parameter()
        z = def_parameter()
        out = def_return()

        @step(uses=["x", "y"], impacts=["out"])
        def _add(self):
            self.out = self.x + self.y

        @step(uses=["z", "out"], impacts=["out"])
        def _subtract(self):
            self.out = self.out - self.z

    test = Test(x=1, y=2, z=3)
    assert test.run() == 0
