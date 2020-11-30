from copy import deepcopy
import pathlib
from inspect import getfullargspec

from attr import attrs, attrib, asdict
from attr.validators import instance_of, optional

from .utils import dispatch_function
from .xlsx import create_xlsx_audit_file


@attrs(slots=True, frozen=True)
class AuditStepConfig:
    show_method_name = attrib(type=bool, default=True)
    show_docstring = attrib(type=bool, default=True)
    show_uses = attrib(type=bool, default=True)
    show_impacts = attrib(type=bool, default=True)
    show_output = attrib(type=bool, default=True)
    show_metadata = attrib(type=bool, default=True)
    nonconstant = attrib(type=dict, factory=dict)
    input_format = attrib(type=str, default="base")
    output_format = attrib(type=str, default="base")


@attrs(slots=True, frozen=True)
class AuditConfig:
    show_signature = attrib(type=bool, default=True)
    show_docstring = attrib(type=bool, default=True)
    show_steps = attrib(type=bool, default=True)
    step_config = attrib(type=AuditStepConfig, default=AuditStepConfig())


def _get_model_output(model):
    steps = []
    for step in model.__footings_steps__:
        method = getattr(model, step)
        step_obj = method.func
        method()
        results = deepcopy(model)
        if hasattr(model, step + "_audit"):
            getattr(results, step + "_audit")()
        step_output = {item: getattr(results, item) for item in step_obj.impacts}
        steps.append(step_output)

    final_output = {asset: getattr(model, asset) for asset in model.__footings_assets__}

    return steps, final_output


@attrs(slots=True, frozen=True)
class AuditStepContainer:
    name = attrib(type=str)
    method_name = attrib(type=str, default=None)
    docstring = attrib(type=str, default=None)
    uses = attrib(type=list, default=None)
    impacts = attrib(type=list, default=None)
    output = attrib(type=dict, default=None)
    metadata = attrib(type=dict, default=None)

    @classmethod
    def create(cls, step, output, config):
        kwargs = {"name": step.name}
        if config.show_method_name:
            kwargs.update({"method_name": step.method_name})
        if config.show_docstring:
            kwargs.update({"docstring": step.docstring})
        if config.show_uses:
            kwargs.update({"uses": step.uses})
        if config.show_impacts:
            kwargs.update({"impacts": step.impacts})
        if config.show_output:
            kwargs.update({"output": output})
        if config.show_metadata:
            kwargs.update({"metadata": step.metadata})
        return cls(**kwargs)

    def as_audit(self):
        return {k: v for k, v in asdict(self).items() if v is not None}


@attrs(slots=True, frozen=True)
class AuditContainer:
    """Container for model audit output."""

    name = attrib(type=str)
    instantiation = attrib(type=dict, validator=instance_of(dict))
    output = attrib(type=dict, validator=instance_of(dict))
    docstring = attrib(type=str, default=None, validator=optional(instance_of(str)))
    signature = attrib(type=str, default=None, validator=optional(instance_of(str)))
    steps = attrib(type=list, default=None, validator=optional(instance_of(list)))
    metadata = attrib(type=dict, default=None, validator=optional(instance_of(dict)))
    config = attrib(
        type=AuditConfig, factory=AuditConfig, validator=instance_of(AuditConfig)
    )

    @classmethod
    def create(cls, model, config=None):
        """Create audit"""
        if config is None:
            config = AuditConfig()
        name = model.__class__.__qualname__
        kwargs = {"name": name, "config": config}
        if config.show_docstring:
            kwargs.update({"docstring": model.__doc__})
        if config.show_signature:
            kwargs.update({"signature": f"{name}{str(model.__signature__)}"})

        kws = getfullargspec(model.__class__).kwonlyargs
        attributes = model._combine_attributes()
        instantiation = {attributes[kw]: deepcopy(getattr(model, kw)) for kw in kws}
        kwargs.update({"instantiation": instantiation})

        step_output, final_output = _get_model_output(model)

        if config.show_steps:
            steps = []
            for step, output in zip(model.__footings_steps__, step_output):
                step_dict = AuditStepContainer.create(
                    getattr(model, step).func, output, config.step_config
                ).as_audit()
                steps.append(step_dict)
            kwargs.update({"steps": steps})

        kwargs.update({"output": final_output})

        return cls(**kwargs)

    def as_audit(self):
        return {k: v for k, v in asdict(self).items() if v is not None}


#########################################################################################
# run_model_audit
#########################################################################################


def run_model_audit(model, file, **kwargs):
    output_ext = pathlib.Path(file).suffix
    config = kwargs.pop("config", None)
    audit_dict = AuditContainer.create(model, config=config).as_audit()
    _run_model_audit(output_ext=output_ext, audit_dict=audit_dict, file=file, **kwargs)


@dispatch_function(key_parameters=("output_ext",))
def _run_model_audit(output_ext, audit_dict, file, **kwargs):
    """test run_model audit"""
    msg = "No registered function based on passed paramters and no default function."
    raise NotImplementedError(msg)


@_run_model_audit.register(output_ext=".xlsx")
def _(audit_dict, file, **kwargs):
    """Run model audit"""

    create_xlsx_audit_file(audit_dict, file, **kwargs)
