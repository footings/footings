from dask import delayed, compute

from footings import model, step, Footing, define_parameter, define_asset
from footings.model_tools import make_foreach_model


@model(steps=["_add"])
class Model(Footing):
    x = define_parameter(dtype=int)
    y = define_parameter(dtype=int)
    total = define_asset(dtype=int)

    @step(uses=["x", "y"], impacts=["total"])
    def _add(self):
        self.total = self.x + self.y


def test_foreach():
    param_list = [{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}, {"x": "5"}]

    foreach_model_1 = make_foreach_model(
        Model, iterator_name="param_list", iterator_params=["x"], iterator_key=["x"]
    )

    success_1, error_1 = foreach_model_1(param_list=param_list, y=1)
    assert success_1 == [2, 3, 4, 5]
    assert error_1[0]["key"] == ["5"]

    foreach_model_2 = make_foreach_model(
        Model,
        iterator_name="param_list",
        iterator_params=["x"],
        iterator_key=["x"],
        success_wrap=sum,
    )

    success_2, error_2 = foreach_model_2(param_list=param_list, y=1)
    assert success_2 == 14
    assert error_2[0]["key"] == ["5"]

    foreach_model_3 = make_foreach_model(
        Model,
        iterator_name="param_list",
        iterator_params=["x"],
        iterator_key=["x"],
        success_wrap=sum,
        delay=delayed,
        compute=compute,
    )

    success_3, error_3 = foreach_model_3(param_list=param_list, y=1)
    assert success_3 == 14
    assert error_3[0]["key"] == ["5"]
