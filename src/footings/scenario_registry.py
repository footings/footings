from functools import partial
from typing import Any

from attr import asdict, attrib, attrs, evolve, make_class
from attr._make import _CountingAttr
from attr.validators import instance_of


def _evolve_scenario(self, scenario: type):
    update = {k: v for k, v in scenario.__dict__.items() if k[0] != "_"}
    return evolve(self.registry["base"], **update)


def _is_attribute(x):
    if not isinstance(x, _CountingAttr):
        return False
    elif "description" not in x.metadata:
        return False
    return True


def def_attribute(default: Any, dtype: type = None, description: str = None):
    """Define an attribute within a scenario.

    :param Any default: The default value for the attribute.
    :param type dtype: The dtype of the attribute.
    :param str description: The attribute description.
    """

    if dtype is None:
        return attrib(default=default, metadata={"description": description})
    return attrib(
        default=default,
        type=dtype,
        validator=instance_of(dtype),
        metadata={"description": description},
    )


@attrs(frozen=True, slots=True)
class ScenarioRegistry:
    """Base class representing a scenario repository."""

    attributes = attrib(type=tuple, init=False, repr=False)
    registry = attrib(factory=dict, init=False, repr=False)

    # def __doc__(self):
    #     return 0

    def get(self, scenario: str):
        """Get a scenario."""
        return asdict(self.registry[scenario])

    def register(self, scenario: type):
        """Register a scenario."""
        self.registry[scenario.__name__] = _evolve_scenario(self, scenario)

    def scenario_keys(self):
        """Show scenario keys."""
        return self.registry.keys()

    def scenario_items(self):
        """Show scenario items."""
        return {k: asdict(v) for k, v in self.registry.items()}


def scenario_registry(cls: type):
    """Decorator to declare scenario registry."""

    def make_scenario_registry(cls):

        attrs = {k: v for k, v in cls.__dict__.items() if _is_attribute(v)}
        attrs2 = attrs.copy()
        attrs2["attributes"] = attrib(
            default=tuple(attrs.keys()), type=tuple, init=False, repr=False
        )
        cls = make_class(
            cls.__name__,
            attrs2,
            bases=(ScenarioRegistry,),
            slots=True,
            frozen=True,
            repr=False,
        )
        cls = cls()  # make instance
        cls.registry["base"] = make_class("base", attrs, slots=True, frozen=True)()
        return cls

    if cls is None:
        return partial(scenario_registry)

    return make_scenario_registry(cls)  # note an instance is returned
