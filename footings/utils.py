"""utils.py"""

from functools import partial

from attr import attrs, attrib
from attr.validators import optional, is_callable, instance_of


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


def _update_registry(registry, keys, function):
    if isinstance(keys, str):
        registry.update({keys: function})
    else:
        for key in keys:
            registry.update({key: function})
    return registry


@attrs(slots=True, frozen=True)
class Dispatcher:
    """Dispatch function"""

    name = attrib(validator=instance_of(str))
    default = attrib(default=None, validator=optional(is_callable()))
    registry = attrib(init=False, repr=False, factory=dict)

    def register(self, keys, function=None):
        """Register a key and function to dispatch on."""
        if function is None:
            return partial(_update_registry, self.registry, keys)
        return _update_registry(self.registry, keys, function)

    def __call__(self, key, *args, **kwargs):
        func = self.registry.get(key, None)
        if func is None:
            if self.default is None:
                msg = f"The key [{key}] does not exist within registry and no default \
                as fallback."
                raise DispatcherKeyError(msg)
            return self.default(*args, **kwargs)
        return func(*args, **kwargs)
