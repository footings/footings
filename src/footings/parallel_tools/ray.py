from typing import Callable, Dict, Optional, Tuple

# import ray
#
# from ..jigs import WrappedModel, MappedModel, ForeachJig
#
# __all__ = ["create_ray_foreach_jig"]
#
#
# def ray_wrapper(func, **kwargs):
#     def inner(func, **kwargs):
#         return ray.remote(func, **kwargs)
#
#     return inner(func, **kwargs).remote


def create_ray_foreach_jig(
    model,
    *,
    iterator_name: str,
    iterator_keys: tuple,
    mapped_keys: Optional[Tuple] = None,
    constant_params: Optional[Tuple] = None,
    pass_iterator_keys: Optional[Tuple] = None,
    success_wrap: Optional[Callable] = None,
    error_wrap: Optional[Callable] = None,
    ray_remote_kwargs: Optional[Dict] = None,
    ray_get_kwargs: Optional[Dict] = None,
):
    """Create a ray backed ForeachJig that runs a WrappedModel or MappedModels for each item in an iterator.

    :param model: The models to call.
    :type model: Union[WrappedModel, MappedModel]
    :param str iterator_name: The name to assign the iterator to be passed (will be used in
        signature of the returned model).
    :param Optional[Tuple] mapped_keys: The keys to be used to lookup the model in mapping.
    :param Optional[Tuple] constant_params: The parameter names which will be constant for all
        items in the iterator.
    :param Optional[Callable] success_wrap: An optional function to call upon running the model
        on the items that returned without error (note if none return without error an empty
        list is returned).
    :param Optional[Callable] error_wrap: An optional function to call upon running the model
        on the items that returned with error (note if none return with error an empty list is
        returned).
    :param Optional[Dict] ray_remote_kwargs: Optional kwargs to pass into ray.remote.
    :param Optional[Dict] ray_get_kwargs: Optional kwargs to pass into ray.get.

    :return: ForeachJig (with updated signature)
    """
    pass


#     if isinstance(model, dict):
#         if mapped_keys is None:
#             msg = (
#                 "When passing a dict of models, the keys used must be set in mapped_keys."
#             )
#             raise ValueError(msg)
#         model = MappedModel.create(
#             model,
#             model_wrapper=WrappedModel,
#             iterator_keys=iterator_keys,
#             mapped_keys=mapped_keys,
#             pass_iterator_keys=pass_iterator_keys,
#             parallel_wrap=ray_wrapper,
#             parallel_kwargs=ray_remote_kwargs,
#         )
#     else:
#         model = WrappedModel(
#             model,
#             iterator_keys=iterator_keys,
#             pass_iterator_keys=pass_iterator_keys,
#             parallel_wrap=ray_wrapper,
#             parallel_kwargs=ray_remote_kwargs,
#         )
#
#     return ForeachJig.create(
#         model=model,
#         iterator_name=iterator_name,
#         constant_params=constant_params,
#         success_wrap=success_wrap,
#         error_wrap=error_wrap,
#         compute=ray.get,
#         compute_kwargs=ray_get_kwargs,
#     )
#
