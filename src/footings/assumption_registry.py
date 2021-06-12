from functools import partial
from inspect import signature
from types import MethodType
from typing import Dict, Union

from attr import attrib, attrs, make_class
from attr.validators import instance_of, is_callable, optional

from .model import Model


def assumption_doc_generator(self):
    def prepare_description(doc):
        ret = ""
        for line in doc.split("\n"):
            ret += line.strip() + "\n"
        return ret

    if self.name != self.assumption.__name__:
        ret = f"**{self.name} [{self.assumption.__name__}] :**\n"
    else:
        ret = f"**{self.name} :**\n"
    if self.description is not None and self.description != "":
        ret += prepare_description(self.description) + "\n"
    elif self.assumption.__doc__ is not None:
        ret += prepare_description(self.assumption.__doc__) + "\n"
    else:
        ret += "\n"
    # if len(self.uses) > 0:
    #     ret += "Uses - \n"
    #     ret += "\n".join(self.uses)
    # if len(self.metadata) > 0:
    #     ret += "Metadata - \n"
    #     ret += "\n".join([f"{k}: {v}" for k, v in self.metadata.items()])
    return ret


@attrs(frozen=True, slots=True)
class Assumption:
    name = attrib(type=str, validator=instance_of(str))
    description = attrib(type=str, validator=instance_of(str))
    assumption = attrib(type=callable, validator=is_callable())
    uses = attrib(type=tuple, validator=instance_of(tuple))
    bounded = attrib(type=bool, validator=instance_of(bool))
    metadata = attrib(type=dict, validator=instance_of(dict))
    doc_generator = attrib(type=callable, validator=is_callable())

    @classmethod
    def create(
        cls,
        *,
        assumption: callable,
        name: Union[str, None] = None,
        bounded: Union[bool, None] = None,
        description: Union[str, None] = None,
        uses: Union[tuple, None] = None,
        cache: Union[tuple, None] = None,
        metadata: Union[dict, None] = None,
        doc_generator: Union[callable, None] = None,
    ):
        if name is None:
            name = assumption.__name__
        if bounded is None:
            bounded = False
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
            bounded=bounded,
            metadata=metadata,
            doc_generator=doc_generator,
        )

    @property
    def __signature__(self):
        return signature(self.assumption)

    @property
    def __doc__(self):
        return self.doc_generator(self)

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
        bounded: Union[bool, None] = None,
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
                bounded=bounded,
                uses=uses,
                metadata=metadata,
                doc_generator=doc_generator,
            )
        self.registry[assumption.__name__] = Assumption.create(
            name=name,
            description=description,
            bounded=bounded,
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
    return doc


@attrs(frozen=True, slots=True)
class AssumptionSet:
    """Base class representing a temporary assumption set."""

    def get(self, assumption: callable):
        """Register an assumption."""

        return getattr(self, assumption)


def make_assumption_set(name: str, description: str, registry: dict):
    """Make an assumption set. That is a child of the parent AssumptionSet."""
    cls = make_class(name=name, attrs={}, bases=(AssumptionSet,), slots=True, frozen=True)
    for k, v in registry.items():
        setattr(
            cls,
            k,
            MethodType(v, cls) if v.bounded else staticmethod(v),
        )
    cls.__doc__ = make_assumption_set_doc(name, description, registry)
    return cls()


@attrs(frozen=True, slots=True)
class AssumptionRegistry:
    """Base class representing an assumption repository."""

    def get(self, assumption_set: str, assumption: str, model: Model = None):
        """Get an assumption from an assumption set."""
        return getattr(self, assumption_set).get(assumption)


def _make_registry_doc(summary: str, assumption_sets: Dict[str, AssumptionSet]):
    if summary is None:
        doc = ""
    else:
        doc = summary + "\n\n"
    doc += ".. rubric:: Assumption Sets\n\n"
    for v in assumption_sets.values():
        doc += v.__doc__

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
