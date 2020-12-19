import inspect
import types
from itertools import product
from functools import partial, update_wrapper
from collections.abc import Iterable
from typing import Callable, Tuple


class DispatchMissingArgumentError(ValueError):
    """Missing Argument to dispatch_function.register"""


def dispatch_function(key_parameters: Tuple, default_function: Callable = None):
    """Transform a function into a generic dispatch function.

    A dispatch function operates similar to `functools.singledispatch` where a generic function
    is recorded and additional functions can be registered and called based on passed arguments.
    However, `functools.singledispatch` calls a registered function based on the type of the first
    argument. Dispatch function calls a registered function based on the key word arguments assigned
    when registering a function. If a registered function does not exist for the passed parameters,
    the default functions is called. To force only registerd functions to be used, have the
    default function `raise NotImplementedError`.

    A dispatch_function is a useful function to replace a block of if else statements that call
    different functions.

    Parameters
    ----------
    key_parameters : tuple
        A tuple of the paramters to use as
    default_function : callable
        The default function to use when the function is called and a regestered function cannot be
        found. function docstring and signature sourced.

    See Also
    --------
    functools.singledispatch

    Examples
    --------
    >>> @dispatch_function(key_parameters=("key",))
    >>> def dispatch(key):
    >>>     return "default"
    >>>
    >>> @dispatch.register(key="x")
    >>> def _():
    >>>     return "x"
    >>>
    >>> @dispatch.register(key="y")
    >>> def _():
    >>>     return "y"
    >>>
    >>> assert dispatch(key="x") == "x"
    >>> assert dispatch(key="y") == "y"
    >>> assert dispatch(key="z") == "default"
    >>> assert dispatch(key=None) == "default"
    """
    if default_function is None:
        return partial(dispatch_function, key_parameters)

    registry = {}

    def register(function: Callable = None, **kwargs):
        """Register a function using a key established by the required parameters.

        Parameters
        ----------
        function : callable
            The function to register.
        **kwargs
            The key_parameters to register for the given function.

        Raises
        ------
        DispatchMissingArgumentError
            Missing Argument to dispatch_function.register.
        """
        if function is None:
            return partial(register, **kwargs)

        items = []
        for key in key_parameters:
            value = kwargs.get(key, None)
            if value is None:
                msg = f"The parameter [{key}] is not a parameter of the instance."
                raise DispatchMissingArgumentError(msg)
            if isinstance(value, str):
                items.append([value])
            elif isinstance(value, Iterable):
                items.append(value)
            else:
                items.append([value])
        keys = list(product(*items))
        for key in keys:
            registry.update({key: function})

    def wrapper(**kwargs):
        orig_kwargs = kwargs.copy()
        key_args = []
        for key in key_parameters:
            try:
                key_args.append(kwargs.get(key))
                kwargs.pop(key)
            except KeyError:
                pass
        key_args = tuple(key_args)
        registry_function = registry.get(key_args, None)
        if registry_function is None:
            return default_function(**orig_kwargs)
        return registry_function(**kwargs)

    wrapper.register = register
    wrapper.registry = types.MappingProxyType(registry)
    wrapper.__signature__ = inspect.signature(default_function)
    update_wrapper(wrapper, default_function)
    return wrapper


def loaded_function(function: Callable = None):
    """Transform a function into a generic loaded function.

    A loaded function is an object that allows hooks to be registered and called either before
    or after the function is called. Hooks are registered as functions and need a positional
    argument of 'start' to call before the function or 'end' to be call after the function.

    This is a useful object to add validation of inputs before the function is called or validation
    of output after the function has been called.

    Parameters
    ----------
    function: callable
        The primary function.

    Examples
    --------
    >>> @loaded_function
    >>> def loaded(a, b):
    >>>     '''Main function'''
    >>>     return a + b
    >>>
    >>> @loaded.register(position="start")
    >>> def _(a, b):
    >>>     b += 1
    >>>     return a, b
    >>>
    >>> @loaded.register(position="end")
    >>> def _(x):
    >>>     return x + 1
    >>>
    >>> with pytest.raises(ValueError):
    >>>
    >>>     @loaded.register(position="zzz")
    >>>     def _(x):
    >>>         pass
    >>>
    >>> assert loaded(a=1, b=1) == 4
    >>> assert loaded(1, 1) == 4
    """
    if function is None:
        return partial(loaded_function)

    # test if function is already loaded
    if hasattr(function, "loaded"):
        loaded = function.loaded
        need_to_register = False
    else:
        loaded = []
        need_to_register = True

    def register(position: str, function: Callable = None):
        """Register a function into the registry.

        Parameters
        ----------
        function : callable
            The function to register.
        position : str
            Use 'end' if adding to the end of the registry or 'start' to add to the begining.

        Raises
        ------
        ValueError
            If position is not 'end' or 'start'.
        """
        if function is None:
            return partial(register, position)
        if position == "end":
            loaded.append(function)
        elif position == "start":
            loaded.insert(0, function)
        else:
            msg = f"The position {position} is not known. Please use 'start' or 'end'."
            raise ValueError(msg)

    def wrapper(*args, **kwargs):
        ret = loaded[0](*args, **kwargs)
        if len(loaded) > 1:
            for func in loaded[1:]:
                if isinstance(ret, tuple):
                    ret = func(*ret)
                else:
                    ret = func(ret)
        return ret

    wrapper.register = register
    wrapper.loaded = loaded
    wrapper.__signature__ = inspect.signature(function)
    update_wrapper(wrapper, function)
    if need_to_register is True:
        wrapper.register(position="end", function=function)
    return wrapper
