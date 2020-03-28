"""footing.py"""

from functools import partial

from attr import attrs, attrib
from attr.validators import instance_of, is_callable

from footings.argument import Argument

#########################################################################################
# established errors
#########################################################################################


class FootingStepNameExists(Exception):
    """Step name already exisits within Footing."""


class FootingUseStepNotPresent(Exception):
    """The step name to use as a dependency is not present."""


#########################################################################################
# footing
#########################################################################################


@attrs(slots=True, frozen=True)
class Use:
    """Use"""

    name = attrib(validator=instance_of(str))


@attrs(slots=True, frozen=True)
class FootingStep:
    """FootingStep"""

    function = attrib(validator=is_callable())
    init_args = attrib(validator=instance_of(dict))
    dependent_args = attrib(validator=instance_of(dict))
    defined_args = attrib(validator=instance_of(dict))
    meta = attrib(validator=instance_of(dict))


def _update_steps(registry, name, function, **kwargs):
    if name is None:
        name = function.__name__
    if name in registry:
        raise FootingStepNameExists(f"The name [{name}] already exists as a step.")
    init_args = kwargs.pop("init_args")
    dependent_args = kwargs.pop("dependent_args")
    defined_args = kwargs.pop("defined_args")
    meta = kwargs.pop("meta")
    step = FootingStep(function, init_args, dependent_args, defined_args, meta)
    registry.update({name: step})


@attrs(slots=True, frozen=True)
class Footing:
    """Footing"""

    name = attrib(validator=instance_of(str))
    arguments = attrib(factory=dict)
    steps = attrib(factory=dict)
    dependencies = attrib(factory=dict)

    def add_step(self, function=None, **kwargs):
        """Add step to footing"""
        name = kwargs.pop("name", None)
        dependencies = set()
        init_args = {}
        dependent_args = {}
        defined_args = {}
        args = kwargs.pop("args", None)
        if args is not None:
            for arg_nm, arg_val in args.items():
                if isinstance(arg_val, Argument):
                    if arg_nm not in self.arguments:
                        self.arguments.update({arg_nm: arg_val})
                    init_args.update({arg_nm: arg_val.name})
                elif isinstance(arg_val, Use):
                    if arg_val.name not in self.steps:
                        msg = f"The step [{arg_val.name}] does not exist."
                        raise FootingUseStepNotPresent(msg)
                    dependent_args.update({arg_nm: arg_val.name})
                    dependencies.add(arg_val.name)
                else:
                    defined_args.update({arg_nm: arg_val})
        self.dependencies.update({name: dependencies})
        meta = kwargs.pop("meta", {})
        if function is None:
            return partial(
                _update_steps,
                self.steps,
                name,
                init_args=init_args,
                dependent_args=dependent_args,
                defined_args=defined_args,
                meta=meta,
            )
        return _update_steps(
            self.steps,
            name,
            function,
            init_args=init_args,
            dependent_args=dependent_args,
            defined_args=defined_args,
            meta=meta,
        )


def footing_from_list(name, steps):
    """Create a footing from list"""
    new_footing = Footing(name=name)
    for step in steps:
        new_footing.add_step(**step)
    return new_footing
