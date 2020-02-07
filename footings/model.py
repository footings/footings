"""model.py"""

# from typing import List
from attr import attrs, attrib, to_dict, make_class
from attr.validators import instance_of, deep_iterable, is_callable


class BaseModel:
    """BaseModel"""

    @classmethod
    def register_scenario(cls, name, **kwargs):
        """Register scenario"""
        return cls, name, kwargs

    def run(self, audit=False, client=None):
        """Run model"""
        return self, audit, client


def _build_attributes(plan):
    return plan


def _build_base(plan):
    return plan


def build_model(name, plan, scenarios=None):
    """Build model"""
    attributes = _build_attributes(plan)
    base = _build_base(plan)
    model = make_class(name, attrs=attributes, bases=base)
    if scenarios is not None:
        for k, v in scenarios.items():
            model.register_scenario(k, **v)
    return model
