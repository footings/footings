from inspect import getfullargspec, signature

from footings.model import (
    model,
    step,
    def_parameter,
    def_return,
)

from footings.jigs import (
    Error,
    WrappedModel,
    MappedModel,
    ForeachJig,
    create_foreach_jig,
)


@model(steps=["_add_a_b"])
class Model1:
    k1 = def_parameter()
    k2 = def_parameter()
    a = def_parameter()
    b = def_parameter()
    r = def_return()

    @step(uses=["a", "b"], impacts=["r"])
    def _add_a_b(self):
        self.r = self.a + self.b


@model(steps=["_subtract_a_b"])
class Model2:
    k1 = def_parameter()
    k2 = def_parameter()
    a = def_parameter()
    b = def_parameter()
    r = def_return()

    @step(uses=["a", "b"], impacts=["r"])
    def _subtract_a_b(self):
        self.r = self.a - self.b


def test_wrapped_model():
    model = WrappedModel(Model1, iterator_keys=("k1",), pass_iterator_keys=("k1",))
    assert model(k1="1", k2="2", a=1, b=2) == 3
    assert signature(model) == signature(Model1)
    assert isinstance(model(k1="k1", a=1, b=2), Error)


def test_mapped_model():
    mapping = {
        "1": Model1,
        "2": Model2,
    }
    model_mapping = MappedModel.create(
        mapping=mapping,
        iterator_keys=("k1",),
        mapped_keys=("k1",),
        pass_iterator_keys=("k1",),
        model_wrapper=WrappedModel,
    )
    assert model_mapping(k1="1", k2="1", a=1, b=2) == 3
    assert model_mapping(k1="2", k2="2", a=1, b=2) == -1
    assert getfullargspec(model_mapping).kwonlyargs == ["k1"]
    assert getfullargspec(model_mapping).varkw == "kwargs"
    assert isinstance(model_mapping(k1="1", a=1, b=2), Error)


def test_foreach_model():
    records = [{"k1": "1", "k2": "1", "a": 1}, {"k1": "2", "k2": "1", "a": 1}]

    # test model
    model = WrappedModel(Model1, iterator_keys=("k1",), pass_iterator_keys=("k1",))
    foreach1 = ForeachJig.create(
        model=model, iterator_name="records", constant_params=("b",)
    )
    assert foreach1(records=records, b=2) == ([3, 3], [])
    assert getfullargspec(foreach1).kwonlyargs == ["records", "b"]

    # test mapping
    mapping = {
        "1": Model1,
        "2": Model2,
    }
    model_mapping = MappedModel.create(
        mapping=mapping,
        iterator_keys=("k1",),
        mapped_keys=("k1",),
        pass_iterator_keys=("k1",),
        model_wrapper=WrappedModel,
    )
    foreach2 = ForeachJig.create(
        model=model_mapping, iterator_name="records", constant_params=("b",)
    )
    assert foreach2(records=records, b=2) == ([3, -1], [])
    assert getfullargspec(foreach1).kwonlyargs == ["records", "b"]


def test_create_foreach_jig():
    records = [{"k1": "1", "k2": "1", "a": 1}, {"k1": "2", "k2": "1", "a": 1}]
    foreach_model = create_foreach_jig(
        Model1,
        iterator_name="records",
        iterator_keys=("k1",),
        mapped_keys=("k1",),
        pass_iterator_keys=("k1",),
        constant_params=("b",),
    )
    assert foreach_model(records=records, b=2) == ([3, 3], [])
