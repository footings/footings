from functools import partial
from inspect import signature
from typing import Dict, Union

from attr import attrib, attrs, make_class
from attr.validators import instance_of, is_callable, optional


def assumption_doc_generator(self):
    def _get(x):
        return f"[get('{x}', *args, **kwargs)]"

    if self.name != self.assumption.__name__:
        ret = f"**{self.name} {_get(self.assumption.__name__)} :**\n"
    else:
        ret = f"**{self.name} :**\n"
    if self.description is not None and self.description != "":
        ret += "" + self.description + "\n\n"
    elif self.assumption.__doc__ is not None:
        ret += "" + self.assumption.__doc__ + "\n\n"
    else:
        ret += "\n"
    # if len(self.uses) > 0:
    #     ret += "Uses - \n"
    #     ret += "\n".join(self.uses)
    # if len(self.metadata) > 0:
    #     ret += "Metadata - \n"
    #     ret += "\n".join([f"{k}: {v}" for k, v in self.metadata.items()])
    return ret


@attrs(frozen=True)  # slots=True is excluded so we can update the signature on init
class Assumption:
    name = attrib(type=str, validator=instance_of(str))
    description = attrib(type=str, validator=instance_of(str))
    uses = attrib(type=tuple, validator=instance_of(tuple))
    assumption = attrib(type=callable, validator=is_callable())
    metadata = attrib(type=dict, validator=instance_of(dict))
    doc_generator = attrib(type=callable, validator=is_callable())

    def __attrs_post_init__(self):
        object.__setattr__(self, "__signature__", signature(self.assumption))
        object.__setattr__(self, "__doc__", self.doc_generator(self))

    @classmethod
    def create(
        cls,
        *,
        assumption: callable,
        name: Union[str, None] = None,
        description: Union[str, None] = None,
        uses: Union[tuple, None] = None,
        cache: Union[tuple, None] = None,
        metadata: Union[dict, None] = None,
        doc_generator: Union[callable, None] = None,
    ):
        if name is None:
            name = assumption.__name__
        if description is None:
            description = ""
        if uses is None:
            uses = tuple(signature(assumption).parameters.keys())
        if metadata is None:
            metadata = {}
        if doc_generator is None:
            doc_generator = assumption_doc_generator
        return cls(
            name=name,
            description=description,
            uses=uses,
            assumption=assumption,
            metadata=metadata,
            doc_generator=doc_generator,
        )

    def __call__(self, *args, **kwargs):
        return self.assumption(*args, **kwargs)


@attrs(frozen=True, slots=True)
class AssumptionSetRegistry:
    """Class representing an assumption set registry."""

    description = attrib(type=str, validator=optional(instance_of(str)))
    registry = attrib(factory=dict, init=False)

    def register(
        self,
        assumption: callable = None,
        *,
        name: Union[str, None] = None,
        description: Union[str, None] = None,
        uses: Union[list, None] = None,
        cache: Union[tuple, None] = None,
        metadata: Union[dict, None] = None,
        doc_generator: callable = None,
    ):
        """Register an assumption."""
        if assumption is None:
            return partial(
                self.register,
                name=name,
                description=description,
                uses=uses,
                metadata=metadata,
                doc_generator=doc_generator,
            )
        self.registry[assumption.__name__] = Assumption.create(
            name=name,
            description=description,
            uses=uses,
            assumption=assumption,
            metadata=metadata,
            doc_generator=doc_generator,
        )
        return assumption


def def_assumption_set(description=None):
    """Define a set withan an assumption registry."""

    return AssumptionSetRegistry(description=description)


def make_assumption_set_doc(name, description, registry):
    if description is not None:
        doc = f"{description}\n\n"
    else:
        doc = ""
    doc += ".. rubric:: Assumptions\n\n"
    for k, v in registry.items():
        lines = v.__doc__.split("\n")
        doc += lines[0] + "\n"
        for line in lines[1:]:
            doc += "\t" + line + "\n"
        doc += "\n"
    return doc


@attrs(frozen=True, slots=True)
class AssumptionSet:
    """Base class representing a temporary assumption set."""

    def get(self, assumption: callable, *args, **kwargs):
        """Register an assumption."""

        return getattr(self, assumption)(*args, **kwargs)


def make_assumption_set(name: str, description: str, registry: dict):
    """Make an assumption set. That is a child of the parent AssumptionSet."""
    cls = make_class(name=name, attrs={}, bases=(AssumptionSet,), slots=True, frozen=True)
    for asn_nm, asn_val in registry.items():
        setattr(cls, asn_nm, staticmethod(asn_val))
    cls.__doc__ = make_assumption_set_doc(name, description, registry)
    return cls()


@attrs(frozen=True, slots=True)
class AssumptionRegistry:
    """Base class representing an assumption repository."""

    def get(self, assumption_set: str, assumption: str, *args, **kwargs):
        """Get an assumption from an assumption set."""

        return getattr(self, assumption_set).get(assumption, *args, **kwargs)


def _make_registry_doc(summary: str, assumption_sets: Dict[str, AssumptionSet]):
    if summary is None:
        doc = ""
    else:
        doc = summary + "\n\n"
    doc += ".. rubric:: Assumption Sets\n\n"

    # doc += "Assumption Sets\n"
    # doc += "---------------\n"
    for v in assumption_sets.values():
        doc += v.__doc__
        doc += "\n\n"

    return doc


def assumption_registry(cls):
    """Decorator to declare assumption registry."""

    def make_assumption_registry(cls):

        assumption_sets = {
            k: make_assumption_set(name=k, description=v.description, registry=v.registry)
            for k, v in cls.__dict__.items()
            if isinstance(v, AssumptionSetRegistry)
        }
        summary = cls.__doc__
        cls = make_class(
            cls.__name__,
            list(assumption_sets.keys()),
            bases=(AssumptionRegistry,),
            slots=True,
            frozen=True,
            repr=False,
        )
        cls.__doc__ = _make_registry_doc(summary, assumption_sets)
        cls.__repr__ = f"{cls.__name__}({', '.join(assumption_sets.keys())})"
        return cls(**assumption_sets)  # note an instance is returned

    if cls is None:
        return partial(assumption_registry)

    return make_assumption_registry(cls)
