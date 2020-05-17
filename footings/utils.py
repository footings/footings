"""Utility classes and functions that support the footings library."""

from typing import Callable, Dict, Tuple
from itertools import product
from functools import partial
from collections.abc import Hashable, Iterable

from attr import attrs, attrib
from attr.validators import optional, is_callable, instance_of, deep_iterable

#########################################################################################
# established errors
#########################################################################################


class DispatchFunctionKeyError(Exception):
    """Key does not exist within dispatch function registry."""


class DispatchFunctionRegisterParameterError(Exception):
    """Parameter does not belong to DispatchFunction."""


#########################################################################################
# DispatchFunction
#########################################################################################


def _update_dispatch_registry(registry, keys, function):
    for key in keys:
        registry.update({key: function})
    return registry


@attrs(slots=True, frozen=True, repr=False)
class DispatchFunction:
    """A function that is disptaches other functions based on established parameters.

    Attributes
    ----------
    name: str
        The name to assign the DispatchFunction
    parameters: tuple
        The parameters by which a key is established
    default: callable, optional
        The default function to run if no key exists within the registry.

    Raises
    ------
    DispatchFunctionRegisterParameterError
        Parameter does not belong to DispatchFunction.
    DispatchFunctionKeyError
        Key does not exist within dispatch function registry.

    Notes
    -----
    The class uses the registry pattern. On initilization of the class, a dict called \n
    registry is created which keeps track of the registered keys to dispatch functions on.

    """

    name: str = attrib(validator=instance_of(str))
    parameters: Tuple = attrib(
        validator=deep_iterable(instance_of(Hashable), instance_of(tuple))
    )
    default: Callable = attrib(default=None, validator=optional(is_callable()))
    registry: Dict = attrib(init=False, repr=False, factory=dict)

    def register(self, function=None, **kwargs):
        """Register a function using a key established by the required parameters.

        Parameters
        ----------
        function : [type], optional
            [description], by default None

        Returns
        -------
        None

        Raises
        ------
        DispatchFunctionRegisterParameterError
            Parameter does not belong to DispatchFunction.
        DispatchFunctionRegisterValueError
            The value passed is not a String or Iterable.

        Examples
        --------
        single_key = DispatchFunction(name="single_key", parameters=("key",))

        @single_key.register(key="x")
        def _():
            return "x"

        multi_key = DispatchFunction(name="multi_key", parameters=("key1", "key2"))

        @multi_key.register(key1="x1", key2="x2")
        def _():
            return "x"
        """
        items = []
        for param in self.parameters:
            value = kwargs.get(param, None)
            if value is None:
                msg = f"The parameter [{param}] is not a parameter of the instance."
                raise DispatchFunctionRegisterParameterError(msg)
            if isinstance(value, str):
                items.append([value])
            elif isinstance(value, Iterable):
                items.append(value)
            else:
                items.append([value])

        keys = list(product(*items))
        if function is None:
            return partial(_update_dispatch_registry, self.registry, keys)
        return _update_dispatch_registry(self.registry, keys, function)

    def __call__(self, *args, **kwargs):
        key = []
        for param in self.parameters:
            key.append(kwargs.get(param))
            kwargs.pop(param)
        key = tuple(key)
        func = self.registry.get(key, None)
        if func is None:
            if self.default is None:
                msg = f"The key {key} does not exist within registry and no default."
                raise DispatchFunctionKeyError(msg)
            return self.default(*args, **kwargs)
        return func(*args, **kwargs)


#########################################################################################
# LoadedFunction
#########################################################################################


def _update_loaded_registry(registry, position, function):
    if position == "end":
        registry.append(function)
    elif position == "start":
        registry.insert(0, function)
    else:
        msg = f"The position {position} is not known. Please use 'start' or 'end'."
        raise ValueError(msg)


@attrs(slots=True, frozen=True, repr=False)
class LoadedFunction:
    """A primary function that can be loaded with additional functions to call before \n
    executing the primary function (i.e., pre_hooks) and/or additional functions to \n
    call after executing the primary function.

    Attributes
    ----------
    name: str
        The name to assign the LoadedFunction.
    function: callable
        The primary function.
    """

    name: str = attrib(validator=instance_of(str))
    function: Callable = attrib(validator=is_callable())
    registry: list = attrib(init=False, repr=False, factory=list)

    def __attrs_post_init__(self):
        self.register(self.function)

    def register(self, function=None, position="end"):
        """Register function"""
        if function is None:
            return partial(_update_loaded_registry, self.registry, position)
        return _update_loaded_registry(self.registry, position, function)

    def __call__(self, *args, **kwargs):
        ret = self.registry[0](*args, **kwargs)
        if len(self.registry) > 1:
            for func in self.registry[1:]:
                if isinstance(ret, tuple):
                    ret = func(*ret)
                else:
                    ret = func(ret)
        return ret


def create_loaded_function(function):
    """Create loaded function."""
    name = function.__name__
    return LoadedFunction(name=name, function=function)
