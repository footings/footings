"""utils.py"""


from itertools import product, starmap
from functools import partial
from collections import namedtuple
from collections.abc import Iterable

from attr import attrs, attrib
from attr.validators import optional, is_callable, instance_of, deep_iterable


class PriorStepError(Exception):
    """Only a name or nprior can be set, not both."""


@attrs(slots=True, frozen=True)
class PriorStep:
    """Get prior step values"""

    # pylint: disable=bare-except
    name = attrib(default=None, kw_only=True, validator=optional(instance_of(str)))
    nprior = attrib(default=None, kw_only=True, validator=optional(instance_of(int)))

    def __attrs_post_init__(self):
        pass

    def get_name(self, list_):
        """Get name from list or priors"""
        if self.nprior is not None:
            return list_[-self.nprior].name
        return self.name


GET_PRIOR_STEP = PriorStep(nprior=1)


class GetTbl:
    """Get tbl name"""


GET_TBL = GetTbl()


class DispatcherKeyError(Exception):
    """Key does not exist within dispatch function registry."""


class DispatcherRegisterTypeError(Exception):
    """The types passed to each keyword in register must be a str or an iterable."""


class DispatcherRegisterParameterError(Exception):
    """The parameter passed to register does not much the parameters of the instance."""


def _update_registry(registry, keys, function):
    for key in keys:
        registry.update({key: function})
    return registry


@attrs(slots=True, frozen=True)
class Dispatcher:
    """Dispatch function"""

    name = attrib(validator=instance_of(str))
    parameters = attrib(validator=deep_iterable(instance_of(str), instance_of(tuple)))
    default = attrib(default=None, validator=optional(is_callable()))
    registry = attrib(init=False, repr=False, factory=dict)

    def register(self, function=None, **kwargs):
        """Register a key and function t o dispatch on."""
        items = []
        for param in self.parameters:
            value = kwargs.get(param, None)
            if value is None:
                raise DispatcherRegisterParameterError(
                    f"The parameter [{param}] is not a parameter of the instance."
                )
            if isinstance(value, str):
                items.append([value])
            elif isinstance(value, Iterable):
                items.append(value)
            else:
                raise DispatcherRegisterTypeError(
                    f"The value for [{param}] is not a str or an iterable."
                )

        keys = list(product(*items))
        if function is None:
            return partial(_update_registry, self.registry, keys)
        return _update_registry(self.registry, keys, function)

    def __call__(self, **kwargs):
        key = []
        for param in self.parameters:
            key.append(kwargs.get(param))
            kwargs.pop(param)
        key = tuple(key)
        func = self.registry.get(key, None)
        if func is None:
            if self.default is None:
                msg = f"The key {key} does not exist within registry and no default."
                raise DispatcherKeyError(msg)
            return self.default(**kwargs)
        return func(**kwargs)
