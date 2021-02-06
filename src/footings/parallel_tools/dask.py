from typing import Callable, Dict, Optional, Tuple

from dask import compute, delayed

from ..jigs import ForeachJig, MappedModel, WrappedModel

__all__ = ["create_dask_foreach_jig"]


def compute_wrapper(output, **compute_kwargs):
    return compute(output, **compute_kwargs)[0]


def create_dask_foreach_jig(
    model,
    *,
    iterator_name: str,
    iterator_keys: tuple,
    mapped_keys: Optional[Tuple] = None,
    constant_params: Optional[Tuple] = None,
    pass_iterator_keys: Optional[Tuple] = None,
    success_wrap: Optional[Callable] = None,
    error_wrap: Optional[Callable] = None,
    dask_delayed_kwargs: Optional[Dict] = None,
    dask_compute_kwargs: Optional[Dict] = None,
):
    """Create a dask backed ForeachJig that runs a WrappedModel or MappedModels for each item in an iterator.

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
    :param Optional[Dict] dask_delayed_kwargs: Optional kwargs to pass into dask.dealyed.
    :param Optional[Dict] dask_compute_kwargs: Optional kwargs to pass into dask.compute.

    :return: ForeachJig (with updated signature)
    """

    if isinstance(model, dict):
        if mapped_keys is None:
            msg = (
                "When passing a dict of models, the keys used must be set in mapped_keys."
            )
            raise ValueError(msg)
        model = MappedModel.create(
            model,
            model_wrapper=WrappedModel,
            iterator_keys=iterator_keys,
            mapped_keys=mapped_keys,
            pass_iterator_keys=pass_iterator_keys,
            parallel_wrap=delayed,
            parallel_kwargs=dask_delayed_kwargs,
        )
    else:
        model = WrappedModel(
            model,
            iterator_keys=iterator_keys,
            pass_iterator_keys=pass_iterator_keys,
            parallel_wrap=delayed,
            parallel_kwargs=dask_delayed_kwargs,
        )

    return ForeachJig.create(
        model=model,
        iterator_name=iterator_name,
        constant_params=constant_params,
        success_wrap=success_wrap,
        error_wrap=error_wrap,
        compute=compute_wrapper,
        compute_kwargs=dask_compute_kwargs,
    )
