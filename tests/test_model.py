from attr import attrib
from numpydoc.docscrape import Parameter
import pytest

from footings.attributes import (
    define_return,
    define_meta,
    define_sensitivity,
    define_parameter,
    define_intermediate,
)

from footings.model import FootingsDoc, model, step, ModelCreationError


def test_model_instantiation():

    with pytest.raises(ModelCreationError):

        # fails due to not subclass of Footing
        @model(steps=["_add"])  # noqa: F841
        class MissingFooting:
            parameter = define_parameter(dtype=int)
            ret = define_return(default=0)

            @step(uses=["parameter"], impacts=["ret"])
            def _add(self):
                self.ret = self.ret + self.parameter

        # fails due to using a value vs using one of define_[return, meta, sensitivity, parameter]
        @model(steps=["_add"])
        class FailUsingValue:
            parameter = define_parameter(dtype=int)
            ret = 0

            @step(uses=["parameter"], impacts=["ret"])
            def _add(self):
                self.ret = self.ret + self.parameter

        # fails due to using attrib() vs using one of define_[return, meta, sensitivity, parameter]
        @model(steps=["_add"])
        class FailUsingAttrib:
            parameter = define_parameter(dtype=int)
            ret = attrib(default=0)

            @step(uses=["parameter"], impacts=["ret"])
            def _add(self):
                self.ret = self.ret + self.parameter

        # fail due to missing at least one attribute defined using define_return()
        @model(steps=["_add"])
        class FailMissingReturn:
            parameter = define_parameter(dtype=int)
            ret = define_parameter(default=0)

            @step(uses=["parameter"], impacts=["ret"])
            def _add(self):
                self.ret = self.ret + self.parameter

        # fail due to missing step as method
        @model(steps=[])
        class FailZeroSteps:
            x = define_parameter()
            y = define_return()

        # fail due to missing step as method
        @model(steps=["_add"])
        class FailMissingStep:
            parameter = define_parameter(dtype=int)
            ret = define_return(default=0)

        # fail due to step not decorated
        @model(steps=["_add"])  # noqa: F841
        class FailStepNotDecorated:
            parameter = define_parameter(dtype=int)
            ret = define_return(default=0)

            def _add(self):
                self.ret = self.ret + self.parameter


def test_model_documentation():
    @model(steps=["_add", "_subtract"])
    class Test:
        parameter = define_parameter(description="This is a parameter.")
        sensitivity = define_sensitivity(default=1, description="This is a sensitivity.")
        meta = define_meta(meta="meta", description="This is meta.")
        ret = define_return(dtype="int", description="This is a return.")

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
        param1 = define_parameter()
        param2 = define_parameter()
        sensitivity1 = define_sensitivity(default=1)
        sensitivity2 = define_sensitivity(default=2)
        meta1 = define_meta(meta="meta1", dtype=str)
        meta2 = define_meta(meta="meta2", dtype=str)
        intermediate1 = define_intermediate()
        intermediate2 = define_intermediate()
        return1 = define_return()
        return2 = define_return()

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
        x = define_parameter()
        y = define_parameter()
        z = define_parameter()
        out = define_return()

        @step(uses=["x", "y"], impacts=["out"])
        def _add(self):
            self.out = self.x + self.y

        @step(uses=["z", "out"], impacts=["out"])
        def _subtract(self):
            self.out = self.out - self.z

    test = Test(x=1, y=2, z=3)
    assert test.run() == 0
