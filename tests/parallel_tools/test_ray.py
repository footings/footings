# import ray

# from footings.model import (
#     model,
#     step,
#     def_parameter,
#     def_return,
# )
# from footings.parallel_tools.ray import create_ray_foreach_jig


# ray.init(local_mode=True)
#
#
# @model(steps=["_add_a_b"])
# class Model1:
#     k1 = def_parameter()
#     k2 = def_parameter()
#     a = def_parameter()
#     b = def_parameter()
#     r = def_return()
#
#     @step(uses=["a", "b"], impacts=["r"])
#     def _add_a_b(self):
#         self.r = self.a + self.b
#
#
# def test_create_ray_foreach_jig():
#     records = [{"k1": "1", "k2": "1", "a": 1}, {"k1": "2", "k2": "1", "a": 1}]
#     foreach_model = create_ray_foreach_jig(
#         Model1,
#         iterator_name="records",
#         iterator_keys=("k1",),
#         pass_iterator_keys=("k1",),
#         constant_params=("b",),
#     )
#     assert foreach_model(records=records, b=2) == ([3, 3], [])
#
