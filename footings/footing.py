"""Defining the Footing class which is the underlying object that represents a pipeline \n
and supports the creation of models."""

from typing import Callable, Optional, Dict

from attr import attrs, attrib
from attr.validators import instance_of, is_callable

from footings.argument import Argument

#########################################################################################
# established errors
#########################################################################################


class FootingStepNameExist(Exception):
    """The step name already exisits within the Footing."""


class FootingStepNameDoesNotExist(Exception):
    """The step name to use as a dependency is not present."""


#########################################################################################
# footing
#########################################################################################


@attrs(slots=True, frozen=True)
class Use:
    """

    Attributes
    ----------
    name: str
        The name of the step within a Footing to use as an argument.
    """

    name: str = attrib(validator=instance_of(str))


@attrs(slots=True, frozen=True)
class FootingStep:
    """A container of attributes representing a step within a Footing.

    Attributes
    ----------
    function: callable
        The callable for a step.
    init_args: dict
        Arguments to callable that will be pulled from the initialization of a Model.
    depdendent_args: dict
        Arguments to callable that will be pulled from other steps within Footing.
    defined_args: dict
        Arguments to callable that have been defined when creating the Footing.
    meta: dict
        A placeholder for meta data.
    """

    function: Callable = attrib(validator=is_callable())
    init_args: Dict = attrib(validator=instance_of(dict))
    dependent_args: Dict = attrib(validator=instance_of(dict))
    defined_args: Dict = attrib(validator=instance_of(dict))
    meta: Dict = attrib(validator=instance_of(dict))


@attrs(slots=True, frozen=True)
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
    meta: dict, optional
        A placeholder for meta data.

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
    meta: Optional[Dict] = attrib(factory=dict)

    def add_step(
        self, name: str, function: callable, args: dict, meta: Optional[Dict] = None
    ):
        """Add a step to the footing.

        Parameters
        ----------
        name : str
            The name of the step.
        function : callable
            The function to call within a step.
        args : dict
            The arguments to passed to the function.
        meta : dict, optional
            A placeholder for meta data.

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
        dependencies = set()
        init_args = {}
        dependent_args = {}
        defined_args = {}
        if args is not None:
            for arg_nm, arg_val in args.items():
                if isinstance(arg_val, Argument):
                    if arg_nm not in self.arguments:
                        self.arguments.update({arg_nm: arg_val})
                    init_args.update({arg_nm: arg_val.name})
                elif isinstance(arg_val, Use):
                    if arg_val.name not in self.steps:
                        msg = f"The step [{arg_val.name}] does not exist."
                        raise FootingStepNameDoesNotExist(msg)
                    dependent_args.update({arg_nm: arg_val.name})
                    dependencies.add(arg_val.name)
                else:
                    defined_args.update({arg_nm: arg_val})
        self.dependencies.update({name: dependencies})
        if meta is None:
            meta = {}
        if name in self.steps:
            raise FootingStepNameExist(f"The name [{name}] already exists as a step.")
        step = FootingStep(function, init_args, dependent_args, defined_args, meta)
        self.steps.update({name: step})


def create_footing_from_list(name: str, steps: list, meta: Optional[Dict] = None):
    """A helper function to create a Footing from a list.

    Parameters
    ----------
    name : str
        The name to assign to the Footing.
    steps : list
        A list of steps to create the Footing.
    meta : Optional[Dict]
        A placeholder for meta data.

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
            "args": {"a": Use("step_1"), "b": Use("step_2"), "arg_c": Argument("c")},
        },
    ]
    footing = create_footing_from_list("footing", steps)
    """
    new_footing = Footing(name=name, meta=meta)
    for step in steps:
        new_footing.add_step(**step)
    return new_footing
