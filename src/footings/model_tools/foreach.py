from inspect import getfullargspec, signature, Parameter
import sys
from traceback import extract_tb, format_list
from typing import Iterable

from attr import attrs, attrib, asdict


@attrs
class ForeachRecordError:
    key = attrib()
    error_type = attrib()
    error_value = attrib()
    error_stacktrace = attrib()


def _make_error_catch_model(model, parameters, key, model_wrap=None):
    def _run_model(**kwargs):
        try:
            ret = model(**{k: v for k, v in kwargs.items() if k in parameters}).run()
        except:
            ex_type, ex_value, ex_trace = sys.exc_info()
            ret = ForeachRecordError(
                key=tuple(kwargs[k] for k in key),
                error_type=ex_type,
                error_value=ex_value,
                error_stacktrace=format_list(extract_tb(ex_trace)),
            )
        return ret

    if model_wrap is callable:
        return model_wrap(_run_model)
    return _run_model


def _make_signature(model, iterator_name, iterator_params):
    sig = signature(model)
    params = set(sig.parameters.keys())
    non_iter_args = [
        v for k, v in sig.parameters.items() if k in params.difference(iterator_params)
    ]
    iter_param = Parameter(
        iterator_name, kind=Parameter.KEYWORD_ONLY, annotation=Iterable
    )
    return sig.replace(
        parameters=[iter_param] + non_iter_args, return_annotation=Iterable
    )


def make_foreach_model(
    model,
    iterator_name: str,
    iterator_params: list,
    iterator_key: list,
    success_wrap: callable = None,
    error_wrap: callable = None,
    delay: callable = None,
    compute: callable = None,
):
    """Make a function that runs a Footings model on each item in an iterator.

    Parameters
    ----------
    model
        The Footings model.
    iterator_name : str
        The name of the iterator parameter which will be passed to the function.
    iterator_params : list
        The model parameters that will be passed in through the iterator.
    iterator_key : list
        The key for each record in the iterator.
    success_wrap : callable, optional
        An optional callable to call on the completed model runs.
    error_wrap : callable, optional
        An optional callable to call on the errors produced.
    delay : callable, optional
        An optional callable to make the model be delayed such as with dask.delayed.
    compute : callable, optional
        An optional callable to compute the results when using delayed such as dask.compute.

    Returns
    -------
    Tuple
        [0] The successfully completed model runs.
        [1] The errors generated running the model.
    """
    model_params = getfullargspec(model).kwonlyargs
    signature = _make_signature(model, iterator_name, iterator_params)
    model = _make_error_catch_model(model, model_params, key=iterator_key)
    if delay is not None:
        model = delay(model)

    def foreach_model(**kwargs):
        iterator = kwargs.pop(iterator_name)
        if not isinstance(iterator, Iterable):
            raise ValueError("The specified iterator object is not an iterator.")
        output = [model(**{**entry, **kwargs}) for entry in iterator]
        if compute is not None:
            output = compute(output)[0]
        successes, errors = [], []
        for result in output:
            error = isinstance(result, ForeachRecordError)
            keep = asdict(result) if error else result
            (errors if error else successes).append(keep)
        if success_wrap is not None and len(successes) > 0:
            successes = success_wrap(successes)
        if error_wrap is not None and len(errors) > 0:
            errors = error_wrap(errors)
        return (successes, errors)

    foreach_model.__signature__ = signature

    return foreach_model
