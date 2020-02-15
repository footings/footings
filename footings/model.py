"""model.py"""

# from typing import List
from inspect import getfullargspec

from dask import threaded, multiprocessing 
from attr import attrs, attrib, make_class

from .parameter import Parameter
from .schema import TblSchema
from .utils import PriorStep

USE_PARTITION_WITH_CLIENT_ONLY = True

class ModelScenarioAlreadyExist(Exception):
    """The scenario already exist"""


class ModelScenarioDoesNotExist(Exception):
    """The scenario does not exist"""


class ModelParameterDoesNotExist(Exception):
    """The parameter does not exist"""


@attrs(slots=True)
class _BaseModel:
    """BaseModel"""

    _scenarios = {}
    _plan = []

    @classmethod
    def register_scenario(cls, name, **kwargs):
        """Register scenario"""
        if name in cls._scenarios:
            raise ModelScenarioAlreadyExist(f"The scenario [{name}] already exist.")
        args = set(getfullargspec(cls))
        unknown = set(kwargs.keys()) - args
        if len(unknown) == 1:
            raise ModelParameterDoesNotExist(f"The parameter [{unknown}] does not exist.")
        if len(unknown) > 1:
            raise ModelParameterDoesNotExist(f"The parameters [{unknown}] do not exist.")
        cls._scenarios.update({name: kwargs})

    @classmethod
    def using_scenario(cls, name, **kwargs):
        """Using scenario"""
        if name not in cls._scenarios:
            raise ModelScenarioDoesNotExist(f"The scenario [{name}] does not exist.")
        return cls(**kwargs, **cls._scenarios.get(name))

    def _run_audit(self, **kwargs):
        """Run audit function"""
        dict_ = {}
        for step in self._plan:
            if isinstance(step, TblSchema):
                dict_.update({step.name: kwargs.get(step.name)})
            else:
                args = {}
                for k, v in step.args.items():
                    if isinstance(v, GetPrior):
                        args.update({k: v.get(dict_)})
                    else:
                        args.update({k: kwargs.get(k)})
                dict_.update({step.name: step.function(**args)})
        return dict_

    def _run_client(self, client, **kwargs):
        """Run client function"""
        raise NotImplementedError()

    def _run_default(self, **kwargs):
        """Run default function"""
        prior = {}
        for step in self._plan:
            if isinstance(step, TblSchema):
                prior.update({step.name: kwargs.get(step.name)})
            else:
                args = {}
                for k, v in step.args.items():
                    if isinstance(v, GetPrior):
                        args.update({k: v.get(prior)})
                    else:
                        args.update({k: kwargs.get(k)})
                prior.update({step.name: step.function(**args)})
        return prior

    def run(self, audit=False, client=None):
        """Run model"""
        args = getfullargspec(self).args
        if audit:
            return self._run_audit(**{arg: getattr(self, arg) for arg in args})
        if client is True:
            return self._run_client(
                **{arg: getattr(self, arg) for arg in args}, client=client
            )
        return self._run_default(**{arg: getattr(self, arg) for arg in args})


def _create_attributes(plan):
    args = plan.get_input_args()
    attributes = {}
    for arg in args:
        attribute = {}
        if isinstance(arg, Parameter):
            if arg.default is not None:
                attribute.update({"default": arg.default})
        attribute.update({"kw_only": True, "validator": arg.create_validator()})
        attributes.update({arg.name: attrib(**attribute)})
    return attributes


def _create_docstring(attributes, description, plan, doc_meta):
    # need to build description
    # need to build parameters
    # need to build returns
    # need to build doc_meta
    return description  # attributes, description, meta


def build_model(name, plan, description=None, doc_meta=None, scenarios=None):
    """Build model"""
    # pylint: disable=protected-access
    attributes = _create_attributes(plan)
    model = make_class(
        name, attrs=attributes, bases=(_BaseModel,), slots=True, frozen=True
    )
    func_default, func_client, func_audit = _create_functions(plan)
    model._plan = plan
    model.__doc__ = _create_docstring(attributes, description, plan, doc_meta)
    if scenarios is not None:
        for k, v in scenarios.items():
            model.register_scenario(k, **v)
    return model
