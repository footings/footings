from footings.model import def_parameter, def_return, model, step
from footings.parallel_tools.dask import create_dask_foreach_jig


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


def test_create_dask_foreach_jig():
    records = [{"k1": "1", "k2": "1", "a": 1}, {"k1": "2", "k2": "1", "a": 1}]
    foreach_model = create_dask_foreach_jig(
        Model1,
        iterator_name="records",
        iterator_keys=("k1",),
        pass_iterator_keys=("k1",),
        constant_params=("b",),
    )
    assert foreach_model(records=records, b=2) == ([3, 3], [])
