from functools import partial
from typing import Tuple, Union

from attr import attrib, attrs, make_class
from attr.validators import instance_of, optional


@attrs(frozen=True, slots=True)
class AssumptionSet:
    """Base class representing an assumption set."""

    name = attrib(type=str, validator=instance_of(str))
    description = attrib(type=str, validator=optional(instance_of(str)))
    registry = attrib(factory=dict, init=False, repr=False)

    def get(self, assumption: str):
        """Get an assumption."""

        return self.registry[assumption]

    def register(self, assumption: callable):
        """Register an assumption."""

        self.registry[assumption.__name__] = assumption

    # def __doc__(self):
    #     # doc = ""
    #     return str(self)


def def_assumption_set(description=None):
    """Define a set withan an assumption registry."""

    return partial(AssumptionSet, description=description)


@attrs(frozen=True, slots=True)
class AssumptionRegistry:
    """Base class representing an assumption repository."""

    def get(self, assumption_set: str, assumption: str):
        """Get an assumption from an assumption set."""

        return getattr(self, assumption_set).get(assumption)

    def register(self, assumption_set: Union[str, Tuple[str]]):
        """Register an assumption to an assumption_set."""

        def inner(assumption):
            if isinstance(assumption_set, str):
                getattr(self, assumption_set).register(assumption)
            elif isinstance(assumption_set, tuple):
                for a in assumption_set:
                    getattr(self, a).register(assumption)
            else:
                msg = "The assumption_set must be type str or tuple not "
                msg += f"{str(type(assumption_set))}."
                raise ValueError(msg)

        return inner


def assumption_registry(cls):
    """Decorator to declare assumption registry."""

    def make_assumption_registry(cls):

        attrs = {
            k: attrib(default=v(name=k), init=False, repr=False)
            for k, v in cls.__dict__.items()
            if isinstance(v, partial)
        }
        cls = make_class(
            cls.__name__,
            attrs,
            bases=(AssumptionRegistry,),
            slots=True,
            frozen=True,
            repr=False,
        )
        return cls

    if cls is None:
        return partial(assumption_registry)

    return make_assumption_registry(cls)()  # note an instance is returned
