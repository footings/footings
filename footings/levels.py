"""levels.py"""

# from typing import List
from attr import attrs, attrib, to_dict
from attr.validators import instance_of, deep_iterable, is_callable

from .schema import ColSchema


def get(name):
    """Get prior step values by name"""
    return name


def get_prior(steps):
    """Get prior step values by count"""
    return steps


GET_PRIOR_STEP = get_prior(steps=1)


@attrs(repr=False)
class TblStep:
    """A step in a case"""

    # pylint: disable=too-many-instance-attributes
    function: callable = attrib(validator=is_callable)
    args: dict = attrib(validator=instance_of(dict))
    name: str = attrib(default=None, kw_only=True, validator=instance_of(str))
    required_columns: list = attrib(
        default=None, kw_only=True, validator=deep_iterable(str, list)
    )
    added_columns: list = attrib(
        default=None, kw_only=True, validator=deep_iterable(ColSchema, list)
    )
    modified_columns: list = attrib(
        default=None, kw_only=True, validator=deep_iterable(str, list)
    )
    removed_columns: list = attrib(
        default=None, kw_only=True, validator=deep_iterable(str, list)
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
        return to_dict(self)


@attrs(repr=False)
class TblFlight:
    """A flight of steps in a case"""

    steps: list = attrib(validator=deep_iterable(TblStep, list))
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
        return to_dict(self)


@attrs(repr=False)
class TblPlan:
    """A list of steps and flights that create a case for a model"""

    levels: list = attrib(validator=deep_iterable((TblStep, TblFlight), list))
    name: str = attrib(default=None, kw_only=True, validator=instance_of(str))
    test_in: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    test_out: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    test_all: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    logging: bool = attrib(default=False, kw_only=True, validator=instance_of(bool))
    tags = attrib(default=None, kw_only=True)

    def __repr__(self):
        return "Case"

    def to_dict(self):
        """Turn step to dict"""
        return to_dict(self)

    def create_tblschema(self):
        """Create TblSchema based on all steps/flights"""
        return self

    def generate_input_info(self):
        """Generate input information for model"""
        return self

    def generate_output_info(self):
        """Generate output information for model"""
        return self

    def generate_process_info(self):
        """Generate process information for model"""
        return self
