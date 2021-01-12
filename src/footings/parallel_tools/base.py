from inspect import signature, Signature, Parameter
from traceback import extract_tb, format_list
from typing import Optional, Callable, Tuple, Dict, Iterable, Union
import sys

from attr import attrs, attrib
from attr.validators import instance_of, is_callable, optional

__all__ = ["create_foreach_model"]


def _exclude_iterator_keys(iterator_keys: tuple, pass_iterator_keys: tuple):
    return set(iterator_keys).difference(pass_iterator_keys)


def _get_key(keys: tuple, **kwargs):
    if len(keys) > 1:
        iter_key = tuple(v for k, v in kwargs.items() if k in keys)
    else:
        iter_key = kwargs.get(keys[0], None)
    return iter_key if iter_key != tuple() else None


def _make_mapping_signature(iterator_keys: tuple):
    params = [Parameter(name=key, kind=Parameter.KEYWORD_ONLY) for key in iterator_keys]
    params.append(Parameter(name="kwargs", kind=Parameter.VAR_KEYWORD))

    return Signature(parameters=params, return_annotation=MappedModel)


def _make_foreach_signature(iterator_name: str, constant_params: tuple):
    params = [Parameter(name=iterator_name, kind=Parameter.KEYWORD_ONLY)]
    for param in constant_params:
        params.append(Parameter(name=param, kind=Parameter.KEYWORD_ONLY))

    return Signature(parameters=params, return_annotation=tuple)


@attrs(frozen=True, slots=True)
class ErrorCatch:
    """Catch for any errors generated when running WrappedModel."""

    key = attrib()
    error_type = attrib()
    error_value = attrib()
    error_stacktrace = attrib()


@attrs(frozen=True, slots=True)
class WrappedModel:
    """Model wrapper to catch errors when instantiating and running a model.

    :param callable model: A model built using the Footings framework.

    :return: The output of the wrapped model when calling model.run() when no
        errors occur. If an error occurs during instantiation or running the model
        an ErrorCatch object is returned.

    """

    model = attrib(type=callable, validator=is_callable())

    @classmethod
    def create(
        cls,
        model,
        *,
        iterator_keys: tuple,
        pass_iterator_keys: Optional[Tuple] = None,
        parallel_wrap: Optional[Callable] = None,
        parallel_kwargs: Optional[Dict] = None,
    ):
        """Create a WrappedModel.

        :param model: The model to wrap.
        :param tuple iterator_keys: The keys identifying the record that will be passed to the model.
        :param Optional[Tuple] pass_iterator_keys: The iterator keys to pass into the model.
        :param Optional[Callable] parallel_wrap: An optional wrapper to make the model parallel (e.g., dask.delayed).
        :param  Optional[Dict] parallel_kwargs: Optional kwargs to pass to parallel_wrap.

        :return: WrappedModel
        """
        if pass_iterator_keys is None:
            pass_iterator_keys = tuple()

        def wrapper(**kwargs):
            try:
                excluded_keys = _exclude_iterator_keys(iterator_keys, pass_iterator_keys)
                model_kwargs = {k: v for k, v in kwargs.items() if k not in excluded_keys}
                ret = model(**model_kwargs).run()
            except:
                ex_type, ex_value, ex_trace = sys.exc_info()
                ret = ErrorCatch(
                    key={k: kwargs[k] for k in iterator_keys},
                    error_type=ex_type,
                    error_value=ex_value,
                    error_stacktrace=format_list(extract_tb(ex_trace)),
                )
            return ret

        if parallel_wrap is not None:
            if parallel_kwargs is None:
                wrapper = parallel_wrap(wrapper)
            else:
                wrapper = parallel_wrap(wrapper, **parallel_kwargs)
        cls.__signature__ = signature(model)
        return cls(model=wrapper)

    def __call__(self, **kwargs):
        return self.model(**kwargs)


@attrs(slots=True, frozen=True)
class MappedModel:
    """A mapping of models to choose from and run based on the specified iterator_keys.

    :param dict mapping: A dictonary mapping of keys to the WrappedModel to be called.
    :param tuple iterator_keys: The key names identifying the record that will be passed to the model.

    :return: The output of the wrapped models when calling model.run() when no
        errors occur. If an error occurs during instantiation or running the model
        an ErrorCatch object is returned.
    """

    mapping = attrib(type=dict, validator=instance_of(dict))
    iterator_keys = attrib(type=tuple, validator=instance_of(tuple))

    @classmethod
    def create(
        cls,
        mapping: dict,
        *,
        model_wrapper: callable,
        iterator_keys: tuple,
        pass_iterator_keys: Optional[Tuple] = None,
        parallel_wrap: Optional[Callable] = None,
        parallel_kwargs: Optional[Dict] = None,
    ):
        """Create a MappedModel.

        :param mapping: A mapping of keys to models to be called.
        :param model_wrapper: The function to be called to turn the model into a WrappedModel.
        :param tuple iterator_keys: The keys identifying the record that will be passed to the model.
        :param Optional[Tuple] pass_iterator_keys: The iterator keys to pass into the model.
        :param Optional[Callable] parallel_wrap: An optional wrapper to make the model parallel (e.g., dask.delayed).
        :param  Optional[Dict] parallel_kwargs: Optional kwargs to pass to parallel_wrap.

        :return: WrappedModel
        """
        kws = {
            "iterator_keys": iterator_keys,
            "pass_iterator_keys": pass_iterator_keys,
            "parallel_wrap": parallel_wrap,
            "parallel_kwargs": parallel_kwargs,
        }
        mapping = {k: model_wrapper(v, **kws) for k, v in mapping.items()}
        sig = _make_mapping_signature(iterator_keys)
        cls.__signature__ = sig

        return cls(iterator_keys=iterator_keys, mapping=mapping)

    def get_model(self, **kwargs):
        """Get model based on pass kwargs."""
        iter_key = _get_key(self.iterator_keys, **kwargs)
        if iter_key is None:
            msg = f"Key was not found using the passed kwargs and iterator keys of [{str(self.iterator_keys)}]."
            raise ValueError(msg)

        model = self.mapping.get(iter_key, None)
        if model is None:
            key = {k: kwargs[k] for k in self.iterator_keys}
            msg = f"Model not found using the key of [{str(key)}]."
            raise KeyError(msg)

        return model

    def __call__(self, **kwargs):
        model = self.get_model(**kwargs)
        return model(**kwargs)


@attrs(frozen=True, slots=True)
class ForeachModel:
    """A model runs a WrappedModel or MappedModels for each item in an iterator.

    :param model: The models to call.
    :type model: Union[WrappedModel, MappedModel]
    :param str iterator_name: The name to assign the iterator to be passed (will be used in
        signature of the returned model).
    :param Optional[Tuple] constant_params: The parameter names which will be constant for all
        items in the iterator.
    :param Optional[Callable] success_wrap: An optional function to call upon running the model
        on the items that returned without error (note if none return without error an empty
        list is returned).
    :param Optional[Callable] error_wrap: An optional function to call upon running the model
        on the items that returned with error (note if none return with error an empty list is
        returned).
    :param Optional[Callable] compute: An optional function to be used to call compute or
        return on the modeled objects. This is to be paired with parallel tools such as
        dask.compute or ray.get.
    :param Optional[Dict] compute_kwargs: Optional kwargs to pass into compute.
    """

    model = attrib(
        type=Union[WrappedModel, MappedModel],
        validator=instance_of((WrappedModel, MappedModel,)),
    )
    iterator_name = attrib(type=str, validator=instance_of(str))
    constant_params = attrib(type=Optional[Tuple], validator=instance_of(tuple))
    success_wrap = attrib(type=Optional[Callable], validator=optional(is_callable()))
    error_wrap = attrib(type=Optional[Callable], validator=optional(is_callable()))
    compute = attrib(type=Optional[Callable], validator=optional(is_callable()))
    compute_kwargs = attrib(type=Optional[Dict], validator=instance_of(dict))

    @classmethod
    def create(
        cls,
        model: Union[WrappedModel, MappedModel],
        iterator_name: str,
        constant_params: Optional[Tuple] = None,
        success_wrap: Optional[Callable] = None,
        error_wrap: Optional[Callable] = None,
        compute: Optional[Callable] = None,
        compute_kwargs: Optional[Dict] = None,
    ):
        """A model runs a WrappedModel or MappedModels for each item in an iterator.

        :param model: The models to call.
        :type model: Union[WrappedModel, MappedModel]
        :param str iterator_name: The name to assign the iterator to be passed (will be used in
            signature of the returned model).
        :param Optional[Tuple] constant_params: The parameter names which will be constant for all
            items in the iterator.
        :param Optional[Callable] success_wrap: An optional function to call upon running the model
            on the items that returned without error (note if none return without error an empty
            list is returned).
        :param Optional[Callable] error_wrap: An optional function to call upon running the model
            on the items that returned with error (note if none return with error an empty list is
            returned).
        :param Optional[Callable] compute: An optional function to be used to call compute or
            return on the modeled objects. This is to be paired with parallel tools such as
            dask.compute or ray.get.
        :param Optional[Dict] compute_kwargs: Optional kwargs to pass into compute.
        """

        if constant_params is None:
            constant_params = tuple()

        if compute_kwargs is None:
            compute_kwargs = {}

        cls.__signature__ = _make_foreach_signature(iterator_name, constant_params)
        return cls(
            model=model,
            iterator_name=iterator_name,
            constant_params=constant_params,
            success_wrap=success_wrap,
            error_wrap=error_wrap,
            compute=compute,
            compute_kwargs=compute_kwargs,
        )

    def __call__(self, **kwargs):
        """Calls the underlying WrappedModel or MappedModel for each item in the named iterator.

        :return: A tuple where the first item are those items that have been successfully ran through
            the underlying models and the second item are those items that failed.
        """
        iterator = kwargs.pop(self.iterator_name)
        if not isinstance(iterator, Iterable):
            raise TypeError("The specified iterator object is not an iterator.")
        output = [self.model(**entry, **kwargs) for entry in iterator]
        if self.compute is not None:
            output = self.compute(output, **self.compute_kwargs)
        successes, errors = [], []
        for result in output:
            (errors if isinstance(result, ErrorCatch) else successes).append(result)
        if self.success_wrap is not None and len(successes) > 0:
            successes = self.success_wrap(successes)
        if self.error_wrap is not None and len(errors) > 0:
            errors = self.error_wrap(errors)
        return (successes, errors)


def create_foreach_model(
    model,
    *,
    iterator_name: str,
    iterator_keys: tuple,
    constant_params: tuple = None,
    pass_iterator_keys: tuple = None,
    success_wrap: callable = None,
    error_wrap: callable = None,
):
    """Create a ForeachModel that runs a WrappedModel or MappedModels for each item in an iterator.

    :param model: The models to call.
    :type model: Union[WrappedModel, MappedModel]
    :param str iterator_name: The name to assign the iterator to be passed (will be used in
        signature of the returned model).
    :param Optional[Tuple] constant_params: The parameter names which will be constant for all
        items in the iterator.
    :param Optional[Callable] success_wrap: An optional function to call upon running the model
        on the items that returned without error (note if none return without error an empty
        list is returned).
    :param Optional[Callable] error_wrap: An optional function to call upon running the model
        on the items that returned with error (note if none return with error an empty list is
        returned).
    :param Optional[Callable] compute: An optional function to be used to call compute or
        return on the modeled objects. This is to be paired with parallel tools such as
        dask.compute or ray.get.
    :param Optional[Dict] compute_kwargs: Optional kwargs to pass into compute.

    :return: ForeachModel (with updated signature)
    """
    if isinstance(model, dict):
        model = MappedModel.create(
            model, iterator_keys=iterator_keys, pass_iterator_keys=pass_iterator_keys
        )
    else:
        model = WrappedModel.create(
            model, iterator_keys=iterator_keys, pass_iterator_keys=pass_iterator_keys
        )

    return ForeachModel.create(
        model=model,
        iterator_name=iterator_name,
        constant_params=constant_params,
        success_wrap=success_wrap,
        error_wrap=error_wrap,
    )