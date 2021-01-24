import inspect
import pytest

from attr import attrs, attrib
from attr.setters import frozen, FrozenAttributeError
from numpydoc.docscrape import Parameter

from footings.model import (
    model,
    step,
    def_return,
    def_intermediate,
    def_meta,
    def_sensitivity,
    def_parameter,
    ModelAttributeType,
    FootingsDoc,
    ModelCreationError,
    ModelRunError,
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
    _get_type(attributes["ret"]) is ModelAttributeType.Return
    _get_type(attributes["placeholder"]) is ModelAttributeType.Intermediate
    _get_type(attributes["meta"]) is ModelAttributeType.Meta
    _get_type(attributes["modifier"]) is ModelAttributeType.Sensitivity
    _get_type(attributes["parameter"]) is ModelAttributeType.Parameter

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


def test_model_instantiation():

    with pytest.raises(ModelCreationError):
        # fails due to using a value vs using one of def_[return, meta, sensitivity, parameter]
        @model(steps=["_add"])
        class FailUsingValue:
            parameter = def_parameter(dtype=int)
            ret = 0

            @step(uses=["parameter"], impacts=["ret"])
            def _add(self):
                self.ret = self.ret + self.parameter

    with pytest.raises(ModelCreationError):
        # fails due to using attrib() vs using one of def_[return, meta, sensitivity, parameter]
        @model(steps=["_add"])
        class FailUsingAttrib:
            parameter = def_parameter(dtype=int)
            ret = attrib(default=0)

            @step(uses=["parameter"], impacts=["ret"])
            def _add(self):
                self.ret = self.ret + self.parameter

    with pytest.raises(ModelCreationError):
        # fail due to missing step as method
        @model(steps=["_add"])
        class FailMissingStep:
            parameter = def_parameter(dtype=int)
            ret = def_return(init_value=0)

    with pytest.raises(ModelCreationError):
        # fail due to step not decorated
        @model(steps=["_add"])  # noqa: F841
        class FailStepNotDecorated:
            parameter = def_parameter(dtype=int)
            ret = def_return(init_value=0)

            def _add(self):
                self.ret = self.ret + self.parameter

    with pytest.raises(TypeError):
        # fail due to step not using uses
        @model(steps=["_add"])  # noqa: F841
        class FailStepNoUses:
            parameter = def_parameter(dtype=int)
            ret = def_return(init_value=0)

            @step(impacts=["ret"])
            def _add(self):
                self.ret = self.ret + self.parameter

    with pytest.raises(TypeError):
        # fail due to step not using impacts
        @model(steps=["_add"])  # noqa: F841
        class FailStepNoImpacts:
            parameter = def_parameter(dtype=int)
            ret = def_return(init_value=0)

            @step(uses=["ret", "parameter"])
            def _add(self):
                self.ret = self.ret + self.parameter

    with pytest.raises(ModelCreationError):
        # fail due to uses x not an attribute
        @model(steps=["_add"])  # noqa: F841
        class FailStepUsesWrong:
            parameter = def_parameter(dtype=int)
            ret = def_return(init_value=0)

            @step(uses=["x"], impacts=["ret"])
            def _add(self):
                self.ret = self.ret + self.parameter

    with pytest.raises(ModelCreationError):
        # fail due to step not using impacts
        @model(steps=["_add"])  # noqa: F841
        class FailStepImpactsWrong:
            parameter = def_parameter(dtype=int)
            ret = def_return(init_value=0)

            @step(uses=["ret", "parameter"], impacts=["x"])
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
    assert init_model.__model_parameters__ == tuple(params.keys())
    sensitivities = {
        "sensitivity1": "sensitivity.sensitivity1",
        "sensitivity2": "sensitivity.sensitivity2",
    }
    assert init_model.__model_sensitivities__ == tuple(sensitivities.keys())
    meta = {"meta1": "meta.meta1", "meta2": "meta.meta2"}
    assert init_model.__model_meta__ == tuple(meta.keys())
    intermediates = {
        "intermediate1": "intermediate.intermediate1",
        "intermediate2": "intermediate.intermediate2",
    }
    assert init_model.__model_intermediates__ == tuple(intermediates.keys())
    returns = {"return1": "return.return1", "return2": "return.return2"}
    assert init_model.__model_returns__ == tuple(returns.keys())
    assert init_model.__model_attribute_map__ == {
        **params,
        **sensitivities,
        **meta,
        **intermediates,
        **returns,
    }


def test_model_steps():

    # with pytest.raises(ModelCreationError):
    #
    #     @model(steps=["_step_1"])

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

    assert Test(x=1, y=2, z=3).run() == 0


def test_model_inheritance():
    @model(steps=["_add"])
    class ModelParent:
        x = def_parameter()
        y = def_parameter()
        out = def_return()

        @step(uses=["x", "y"], impacts=["out"])
        def _add(self):
            self.out = self.x + self.y

    assert ModelParent(x=1, y=2).run() == 3

    @model(steps=["_add", "_subtract"])
    class ModelChild(ModelParent):
        z = def_parameter()

        @step(uses=["z", "out"], impacts=["out"])
        def _subtract(self):
            self.out = self.out - self.z

    assert ModelChild(x=1, y=2, z=3).run() == 0


def test_model_run():

    with pytest.raises(ModelRunError):

        @model
        class ModelNoSteps:
            parameter = def_parameter(dtype=int)
            ret = def_return(init_value=0)

        ModelNoSteps(parameter=1).run()

    with pytest.raises(ModelRunError):

        @model(steps=["_add"])
        class ModelNoSteps:
            parameter = def_parameter(dtype=int)
            intermediate = def_intermediate(init_value=0)

            @step(uses=["parameter"], impacts=["intermediate"])
            def _add(self):
                self.intermediate = self.parameter

        ModelNoSteps(parameter=1).run()
