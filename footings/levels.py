"""levels.py"""

# from typing import List
from attr import attrs, attrib
from attr.validators import instance_of, deep_iterable, is_callable, optional
from functools import partial

from .parameter import Parameter
from .schema import ColSchema, TblSchema
from .utils import PriorStep

@attrs(slots=True, frozen=True, repr=False)
class TblStep:
    """A step in a case"""

    # pylint: disable=too-many-instance-attributes
    function: callable = attrib(validator=is_callable())
    args: dict = attrib(validator=instance_of(dict))
    name: str = attrib(kw_only=True, validator=instance_of(str))
    required_columns: list = attrib(
        default=None,
        kw_only=True,
        validator=optional(deep_iterable(instance_of(str), instance_of(list))),
    )
    added_columns: list = attrib(
        default=None,
        kw_only=True,
        validator=optional(deep_iterable(instance_of(ColSchema), instance_of(list))),
    )
    modified_columns: list = attrib(
        default=None,
        kw_only=True,
        validator=optional(deep_iterable(instance_of(str), instance_of(list))),
    )
    removed_columns: list = attrib(
        default=None,
        kw_only=True,
        validator=optional(deep_iterable(instance_of(str), instance_of(list))),
    )
    partition: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    collapse: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    test_in: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    test_out: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    logging: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    tags = attrib(default=None, kw_only=True)

    def __repr__(self):
        return "Step"

    def to_dict(self):
        """Turn step to dict"""
        return self


@attrs(slots=True, frozen=True, repr=False)
class TblFlight:
    """A flight of steps in a case"""

    steps: list = attrib(validator=deep_iterable(instance_of(TblStep), instance_of(list)))
    name: str = attrib(validator=instance_of(str))
    test_in: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    test_out: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    test_all: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    logging: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    tags = attrib(default=None, kw_only=True)

    def __repr__(self):
        return "Flight"

    def to_dict(self):
        """Turn step to dict"""
        return self


@attrs(slots=True, frozen=True, repr=False)
class TblPlan:
    """A list of steps and flights that create a case for a model"""

    name: str = attrib(validator=instance_of(str))
    levels: list = attrib()
    test_in: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    test_out: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    test_all: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    logging: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    tags = attrib(default=None, kw_only=True)

    @levels.validator
    def _levels_validator(self, attribute, value):
        True

    def __repr__(self):
        return "Case"

    def to_dict(self):
        """Turn  step to dict"""
        return self

    def to_task_graph(self):
        """To custom task graph"""
        tables = {}
        params = {}
        funcs = {}
        for idx, level in enumerate(self.levels):
            if isinstance(level, TblSchema):
                tables.update({level.name: (getattr, "self", level.name)})
            else:
                partial_func = partial(level.function)
                param_nms = []
                for k, v in level.args.items():
                    if isinstance(v, Parameter):
                        params.update({k: (getattr, "self", k)})
                        param_nms.append(k)
                    elif isinstance(v, PriorStep):
                        if idx == 0:
                            raise ValueError()
                        param_nms.append(v.get_name(self.levels[:idx]))
                    else:
                        partial_func(**{k: v})
                funcs.update({level.name: (partial_func, *param_nms)})
        return {**tables, **params, **funcs}

    def create_tblschema(self):
        """Create TblSchema based on all steps/flights"""
        return self

    def get_input_args(self):
        """Generate input information for model"""
        args = []
        for step in self.levels:
            if isinstance(step, TblSchema):
                args.append(step)
            elif isinstance(step, TblStep):
                for v in step.args.values():
                    if isinstance(v, Parameter):
                        args.append(v)
            elif isinstance(step, TblFlight):
                raise NotImplementedError()
        return args

    def generate_output_info(self):
        """Generate output information for model"""
        return self

    def generate_process_info(self):
        """Generate process information for model"""
        return self
