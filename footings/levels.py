"""levels.py"""

# from typing import List
from attr import attrs, attrib
from attr.validators import instance_of, deep_iterable, is_callable, optional

from .parameter import Parameter
from .schema import ColSchema, TblSchema
from .task_graph import to_task_graph


@attrs(slots=True, frozen=True, repr=False)
class TblStep:
    """A step in a case"""

    # pylint: disable=too-many-instance-attributes
    task: callable = attrib(validator=is_callable())
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
    returned_columns: list = attrib(
        default=None,
        kw_only=True,
        validator=optional(deep_iterable(instance_of(ColSchema), instance_of(list))),
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


@attrs(slots=True, frozen=True)
class TblPlan:
    """A list of steps and flights that create a case for a model"""

    name: str = attrib(validator=instance_of(str))
    tbl: TblSchema = attrib(kw_only=True, validator=instance_of(TblSchema))
    levels: list = attrib(
        kw_only=True, validator=deep_iterable(instance_of(TblStep), instance_of(list))
    )
    logging: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    tags = attrib(default=None, kw_only=True)

    @levels.validator
    def _levels_validator(self, attribute, value):
        True

    def __repr__(self):
        return "Plan"

    def to_dict(self):
        """Turn  step to dict"""
        return self

    def to_task_graph(self, method="dd"):
        """Create dask task graph"""
        return to_task_graph(self, method)

    def to_tbl_schema(self, position=None):
        """Create TblSchema based on all steps/flights"""
        return self, position

    def get_parameters(self):
        """Get input parameters"""
        params = []
        for level in self.levels:
            for v in level.args.values():
                if isinstance(v, Parameter):
                    params.append(v)
        return params

    def generate_output_info(self):
        """Generate output information for model"""
        return self

    def generate_process_info(self):
        """Generate process information for model"""
        return self
