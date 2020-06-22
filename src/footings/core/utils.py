"""Utility classes and functions that support the footings library."""

from typing import Callable, Dict, Tuple, Optional
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
    """A function that dispatches other functions based on set parameters.

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
    The class uses the registry pattern. On initilization of the class, a dict called
    registry is created which keeps track of the registered keys to dispatch functions on.

    Examples
    --------
    >>> single_key = DispatchFunction(name="single_key", parameters=("key",))
    >>>
    >>> @single_key.register(key="x")
    >>> def _():
    >>>     return "x"
    >>>
    >>> assert single_key(key="x") == "x"
    >>>
    >>> multi_key = DispatchFunction(name="multi_key", parameters=("key1", "key2"))
    >>>
    >>> @multi_key.register(key1="x1", key2="x2")
    >>> def _():
            return "x"
    >>>
    >>> assert single_key(key1="x1", key2="x2") == "x"
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
        function : callable
            The function to register.
        **kwargs
            The parameter keys to register for the given function.


        Returns
        -------
        None

        Raises
        ------
        DispatchFunctionRegisterParameterError
            Parameter does not belong to DispatchFunction.
        DispatchFunctionRegisterValueError
            The value passed is not a String or Iterable.
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


def create_dispatch_function(
    name: str, parameters: tuple, default: Optional[Callable] = None
) -> DispatchFunction:
    """A factory function to create a DispatchFunction

    Parameters
    ----------
    name: str
        The name to assign the DispatchFunction
    parameters: tuple
        The parameters by which a key is established
    default: callable, optional
        The default function to run if no key exists within the registry.

    Returns
    -------
    DispatchFunction

    See Also
    --------
    footings.utils.DispatchFunction

    Examples
    --------
    >>> single_key = create_dispatch_function(name="single_key", parameters=("key",))
    >>>
    >>> @single_key.register(key="x")
    >>> def _():
    >>>     return "x"
    >>>
    >>> assert single_key(key="x") == "x"
    >>>
    >>> multi_key = create_dispatch_function(name="multi_key", parameters=("key1", "key2"))
    >>>
    >>> @multi_key.register(key1="x1", key2="x2")
    >>> def _():
    >>>     return "x"
    >>>
    >>> assert single_key(key1="x1", key2="x2") == "x"
    """
    return DispatchFunction(name=name, parameters=parameters, default=default)


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
    """A function that can be loaded with additional functions to be called all at once.

    The initial function is the primary function and any added functions can be though of as
    pre hooks if added before primary (i.e., position=start) and post hooks if added after
    the primary function (i.e., position=end).

    A LoadedFunction has the added benefit of being recognized when auditing models as
    a function with multiple internal calls.

    Attributes
    ----------
    name: str
        The name to assign the LoadedFunction.
    function: callable
        The primary function.

    Notes
    -----
    The class uses the registry pattern. On initilization of the class, a list called
    registry is created which records and orders the functions to be called.

    Examples
    --------
    >>> def test_primary(x):
    >>>     return x
    >>>
    >>> test = LoadedFunction(name="single_key", function=test_primary)
    >>>
    >>> @test.register
    >>> def _(x):
    >>>     return x > 3
    >>>
    >>> @test.register(position="start")
    >>> def _(x):
    >>>     return x + 1
    >>>
    >>> assert test(2) == False
    >>> assert test(3) == True
    """

    name: str = attrib(validator=instance_of(str))
    function: Callable = attrib(validator=is_callable())
    registry: list = attrib(init=False, repr=False, factory=list)

    def __attrs_post_init__(self):
        self.register(self.function)

    def register(self, function=None, position="end"):
        """Register a function into the registry.

        Parameters
        ----------
        function : callable
            The function to register.
        position : str
            Use end if adding to the end of the registry or start to add to the begining.

        Returns
        -------
        None
        """
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


def create_loaded_function(function: callable) -> LoadedFunction:
    """A factory function to create a LoadedFunction

    Parameters
    ----------
    function: callable
        The primary function.

    Returns
    -------
    LoadedFunction

    See Also
    --------
    footings.utils.LoadedFunction

    Examples
    --------
    >>> def test_primary(x):
    >>>     return x
    >>>
    >>> test = LoadedFunction(name="single_key", function=test_primary)
    >>>
    >>> @test.register
    >>> def _(x):
    >>>     return x > 3
    >>>
    >>> @test.register(position="start")
    >>> def _(x):
    >>>     return x + 1
    >>>
    >>> assert test(2) == False
    >>> assert test(3) == True
    """
    name = function.__name__
    return LoadedFunction(name=name, function=function)
