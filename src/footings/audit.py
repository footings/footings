import pathlib
from copy import deepcopy
from inspect import getfullargspec, signature

from attr import asdict, attrib, attrs
from attr.validators import instance_of, optional

from .io import create_footings_json_file, create_footings_xlsx_file
from .utils import dispatch_function


def _make_signature(model):
    sig = model.__class__.__qualname__
    sig += str(signature(model.__class__))
    return sig[: (sig.find(")") + 1)]


@attrs(slots=True, frozen=True)
class AuditStepConfig:
    show_method_name = attrib(type=bool, default=True)
    show_docstring = attrib(type=bool, default=True)
    show_uses = attrib(type=bool, default=True)
    show_impacts = attrib(type=bool, default=True)
    show_output = attrib(type=bool, default=True)
    show_metadata = attrib(type=bool, default=True)


@attrs(slots=True, frozen=True)
class AuditConfig:
    show_signature = attrib(type=bool, default=True)
    show_docstring = attrib(type=bool, default=True)
    show_steps = attrib(type=bool, default=True)
    step_config = attrib(type=AuditStepConfig, default=AuditStepConfig())


def _get_model_output(model):
    steps = []
    for step in model.__model_steps__:
        method = getattr(model, step)
        step_obj = method.func
        method()
        results = deepcopy(model)
        if hasattr(model, step + "_audit"):
            getattr(results, step + "_audit")()
        step_output = {
            item: getattr(results, item.split(".")[1]) for item in step_obj.impacts
        }
        steps.append(step_output)

    final_output = {ret: getattr(model, ret) for ret in model.__model_returns__}

    return steps, final_output


@attrs(slots=True, frozen=True)
class AuditStepContainer:
    name = attrib(type=str)
    method_name = attrib(type=str, default=None)
    docstring = attrib(type=str, default=None)
    uses = attrib(type=tuple, default=None)
    impacts = attrib(type=tuple, default=None)
    output = attrib(type=dict, default=None)
    metadata = attrib(type=dict, default=None)
    config = attrib(
        type=AuditStepConfig,
        factory=AuditStepConfig,
        validator=instance_of(AuditStepConfig),
    )

    @classmethod
    def create(cls, step, output, config):
        kwargs = {"name": step.name, "config": config}
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

    def as_dict(self):
        d = asdict(self, recurse=True, retain_collection_types=True)
        del d["config"]
        return {k: v for k, v in d.items() if v is not None}


@attrs(slots=True, frozen=True)
class AuditContainer:
    """Container for model audit output."""

    name = attrib(type=str)
    instantiation = attrib(type=dict, validator=instance_of(dict))
    output = attrib(type=dict, validator=instance_of(dict))
    docstring = attrib(type=str, default=None, validator=optional(instance_of(str)))
    signature = attrib(type=str, default=None, validator=optional(instance_of(str)))
    steps = attrib(type=dict, default=None, validator=optional(instance_of(dict)))
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
            kwargs.update({"signature": f"{_make_signature(model)}"})

        kws = getfullargspec(model.__class__).kwonlyargs
        attributes = model.__model_attribute_map__
        instantiation = {attributes[kw]: deepcopy(getattr(model, kw)) for kw in kws}
        kwargs.update({"instantiation": instantiation})

        step_output, final_output = _get_model_output(model)

        if config.show_steps:
            steps = {
                step: AuditStepContainer.create(
                    getattr(model, step).func, output, config.step_config
                )
                for step, output in zip(model.__model_steps__, step_output)
            }
            kwargs.update({"steps": steps})

        kwargs.update({"output": final_output})

        return cls(**kwargs)

    def as_dict(self, include_config=True):
        d = {"name": self.name}
        if self.signature is not None:
            d.update({"signature": self.signature})
        if self.docstring is not None:
            d.update({"docstring": self.docstring})
        if self.instantiation is not None:
            d.update({"instantiation": self.instantiation})
        if self.steps is not None:
            d.update({"steps": {k: v.as_dict() for k, v in self.steps.items()}})
        if self.output is not None:
            d.update({"output": self.output})
        if self.metadata is not None:
            d.update({"metadata": self.metadata})
        if include_config is True:
            d.update({"config": asdict(self.config)})
        return d


#########################################################################################
# run_model_audit
#########################################################################################


def run_model_audit(model, file=None, **kwargs):
    config = kwargs.pop("config", None)
    if file is None:
        return AuditContainer.create(model, config=config)
    file_ext = pathlib.Path(file).suffix
    _run_model_audit(file_ext=file_ext, model=model, file=file, config=config, **kwargs)


@dispatch_function(key_parameters=("file_ext",))
def _run_model_audit(file_ext, model, file, config, **kwargs):
    """test run_model audit"""
    msg = "No registered function based on passed paramters and no default function."
    raise NotImplementedError(msg)


@_run_model_audit.register(file_ext=".xlsx")
def _(model, file, config, **kwargs):
    audit_dict = AuditContainer.create(model, config=config).as_dict()
    create_footings_xlsx_file(audit_dict, file, **kwargs)


@_run_model_audit.register(file_ext=".json")
def _(model, file, config, **kwargs):
    audit_dict = AuditContainer.create(model, config=config).as_dict()
    create_footings_json_file(audit_dict, file, **kwargs)
