from typing import Callable, Dict, Any

from attr import attrs, attrib
from attr.validators import instance_of, is_callable

from .argument import Argument


#########################################################################################
# established errors
#########################################################################################


class FootingDependentGetError(Exception):
    """Footing Dependent get attribute or key error."""


class FootingStepNameExist(Exception):
    """The step name already exisits within the Footing."""


class FootingStepNameDoesNotExist(Exception):
    """The step name to use as a dependency is not present."""


class FootingReserveWordError(Exception):
    """Argument name is a reserve word."""


#########################################################################################
# footing
#########################################################################################


FOOTINGS_RESERVED_WORDS = [
    "scenarios",
    "arguments",
    "steps",
    "dependencies",
    "dependency_index",
]


@attrs(slots=True, frozen=True, repr=False)
class Dependent:
    """A dependent marks an object as a child of an earlier computed step within a model.

    When an argument within a defined step is set to a Dependent of another step, when the step is called,
    the model will input the output of the dependent step.

    get_attr or get_key can be set to retrieve an attribute or a key from a dependent step.

    Attributes
    ----------
    name : str
        The name of the step within a Footing to use as an argument.
    get_attr : Any
        Any attribute to get from named dependent.
    get_key : Any
        Any key to get from named dependent.

    Raises
    ------
    ValueError
        Error raised if both get_attr and get_key are not None
    """

    name: str = attrib(validator=instance_of(str))
    get_attr: Any = attrib(default=None, kw_only=True)
    get_key: Any = attrib(default=None, kw_only=True)

    def __attrs_post_init__(self):
        if self.get_attr is not None and self.get_key is not None:
            msg = "Both get_attr and get_key cannot be None."
            raise ValueError(msg)

    def get_value(self, val: Any):
        """Get value from dependence"""
        if self.get_attr is not None:
            if not hasattr(val, self.get_attr):
                msg = f"The attribute [{self.get_attr}] does not exist within val."
                raise FootingDependentGetError(msg)
            ret = getattr(val, self.get_attr)
        elif self.get_key is not None:
            if not hasattr(val, "__getitem__"):
                msg = "The object val does not have a __getitem__ method."
                raise FootingDependentGetError(msg)
            if self.get_key not in val:
                msg = "The key [{self.get_key}] does not exist within val."
                raise FootingDependentGetError(msg)
            ret = val[self.get_key]
        else:
            ret = val
        return ret


def use(name: str, get_attr: Any = None, get_key: Any = None) -> Dependent:
    """A factory function to create a Dependent.

    A dependent marks an object as a child of an earlier computed step within a model.

    Parameters
    ----------
    name : str
        The name of the step within a Footing to use as an argument.
    get_attr : Any
        Any attribute(s) to get from listed step.
    get_key : Any
        Any key(s) to get from list step.

    See Also
    --------
    footings.footing.Dependent
    """
    return Dependent(name, get_attr=get_attr, get_key=get_key)


@attrs(slots=True, frozen=True, repr=False)
class FootingStep:
    """A container of attributes representing a step within a Footing.

    Attributes
    ----------
    function: callable
        The callable for a step.
    init_args: dict
        Arguments to callable that will be pulled from the initialization of a Model.
    dependent_args: dict
        Arguments to callable that will be pulled from other steps within Footing.
    defined_args: dict
        Arguments to callable that have been defined when creating the Footing.
    """

    function: Callable = attrib(validator=is_callable())
    init_args: Dict = attrib(validator=instance_of(dict))
    dependent_args: Dict = attrib(validator=instance_of(dict))
    defined_args: Dict = attrib(validator=instance_of(dict))


@attrs(slots=True, frozen=True, repr=False)
class Footing:
    """The foundational object to build a model.

    A footing is a registry of function calls which records -
    - the function to be called,
    - the arguments that will be part of the initilization of a model,
    - the arguments to get values from other steps, and
    - the arguments that are already defined.

    Attributes
    ----------
    name: str
        The name to assign to the footing.
    arguments: dict
        A dict that keeps record of arguments that will be used for initilization of a model.
    steps: dict
        A dict acting as a registry of steps where the values are FootingSteps.
    dependencies: dict
        A dict recording the dependencies between steps.

    Raises
    ------
    FootingStepNameDoesNotExist
        The step name to use as a dependency is not present.
    FootingStepNameExist
        The step name already exisits within the Footing.

    See Also
    --------
    create_footing_from_list
    """

    name: str = attrib(validator=instance_of(str))
    arguments: Dict = attrib(factory=dict)
    steps: Dict = attrib(factory=dict)
    dependencies: Dict = attrib(factory=dict)

    def add_step(self, name: str, function: callable, args: dict):
        """Add a step to the footing.

        Parameters
        ----------
        name : str
            The name of the step.
        function : callable
            The function to call within a step.
        args : dict
            The arguments to passed to the function.

        Returns
        -------
        None
            A step is recorded within the Footing registry (i.e., steps).

        Raises
        ------
        FootingStepNameDoesNotExist
            The step name to use as a dependency is not present.
        FootingStepNameExist
            The step name already exisits within the Footing.
        """
        if name in self.steps:
            raise FootingStepNameExist(f"The name [{name}] already exists as a step.")
        dependencies = set()
        init_args = {}
        dependent_args = {}
        defined_args = {}
        if args is not None:
            for arg_nm, arg_val in args.items():
                if isinstance(arg_val, Argument):
                    if arg_val.name in FOOTINGS_RESERVED_WORDS:
                        msg = f"The [{arg_val.name}] is a reserve word."
                        msg += "Use a different argument name."
                        raise FootingReserveWordError(msg)
                    if arg_val.name not in self.arguments:
                        self.arguments.update({arg_val.name: arg_val})
                    init_args.update({arg_nm: arg_val.name})
                elif isinstance(arg_val, Dependent):
                    if arg_val.name not in self.steps:
                        msg = f"The step [{arg_val.name}] does not exist."
                        raise FootingStepNameDoesNotExist(msg)
                    dependent_args.update({arg_nm: arg_val})
                    dependencies.add(arg_val.name)
                else:
                    defined_args.update({arg_nm: arg_val})
        self.dependencies.update({name: dependencies})
        step = FootingStep(function, init_args, dependent_args, defined_args)
        self.steps.update({name: step})


def create_footing_from_list(name: str, steps: list):
    """A helper function to create a Footing from a list.

    Parameters
    ----------
    name : str
        The name to assign to the Footing.
    steps : list
        A list of steps to create the Footing.

    Returns
    -------
    Footing
        A Footing where the passed steps are registered within the object.

    See Also
    --------
    Footing

    Examples
    --------
    steps = [
        {
            "name": "step_1",
            "function": lambda a, add: a + add,
            "args": {"arg_a": Argument("a"), "add": 1},
        },
        {
            "name": "step_2",
            "function": lambda b, subtract: b - subtract,
            "args": {"arg_b": Argument("b"), "subtract": 1},
        },
        {
            "name": "step_3",
            "function": lambda a, b, c: a + b + c,
            "args": {"a": Dependent("step_1"), "b": Dependent("step_2"), "arg_c": Argument("c")},
        },
    ]
    footing = create_footing_from_list("footing", steps)
    """
    new_footing = Footing(name=name)
    for step in steps:
        new_footing.add_step(**step)
    return new_footing
