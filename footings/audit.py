"""Function and classes for auditing models."""

import json

from attr import attrs, attrib
import attr
import pandas as pd
from numpydoc.docscrape import FunctionDoc
from openpyxl.styles import NamedStyle, Font

from .utils import LoadedFunction, DispatchFunction
from .to_xlsx import XlsxWorkbook


def _step_to_audit_format(step):
    """To audit format"""

    def _format_docstring(docstring):
        doc_split = docstring.split("\n")
        if doc_split[0].strip() == "":
            doc_split = doc_split[1:]
        indent = len(doc_split[0]) - len(doc_split[0].lstrip())
        return "\n".join([line[indent:] for line in doc_split])

    step_dict = {}
    docstring = FunctionDoc(step.function)
    step_dict["Signature"] = docstring["Signature"]
    step_dict["Summary"] = docstring["Summary"][0]
    step_dict["Docstring"] = _format_docstring(step.function.__doc__)
    step_dict["Returns"] = docstring["Returns"][0].type
    return step_dict


def _get_model_output(model):
    output = {}
    for k, v in model.steps.items():
        if isinstance(v.function, LoadedFunction):
            init_args = {k: getattr(model, v) for k, v in v.init_args.items()}
            dependent_args = {k: output[v.name] for k, v in v.dependent_args.items()}
            output.update(
                {k: v.function(**init_args, **dependent_args, **v.defined_args)}
            )
        else:
            init_args = {k: getattr(model, v) for k, v in v.init_args.items()}
            dependent_args = {k: output[v.name] for k, v in v.dependent_args.items()}
            output.update(
                {k: v.function(**init_args, **dependent_args, **v.defined_args)}
            )
    return output


def _get_dtype(dtype):
    if dtype is None:
        return ""
    return dtype.__class__.__name__


def _create_argument_output(arguments):
    """Create argument output"""
    return {k: attr.asdict(v) for k, v in arguments.items()}


def _create_argument_summary(arguments):
    return [
        {
            "Argument": argument["name"],
            "Type": _get_dtype(argument["dtype"]),
            "Description": argument["description"],
        }
        for argument in arguments.values()
    ]


def _create_step_output(steps, output):
    """Create step output"""
    return_dict = {}
    for k, v in steps.items():
        step_dict = {}
        audit = _step_to_audit_format(v)
        step_dict["Signature"] = audit["Signature"]
        step_dict["Docstring"] = audit["Docstring"]
        step_dict["Summary"] = audit["Summary"]
        step_dict["Returns"] = audit["Returns"]
        step_dict["Output"] = output[k]
        return_dict[k] = step_dict
    return return_dict


def _create_step_summary(steps):
    return [
        {
            "Step": step_key,
            "Return Type": step_val["Returns"],
            "Description": step_val["Summary"],
        }
        for step_key, step_val in steps.items()
    ]


@attrs(slots=True, frozen=True, repr=False)
class ModelAudit:
    """Container for model audit output."""

    model_name: dict = attrib()
    arguments_summary: dict = attrib()
    arguments: dict = attrib()
    steps_summary: dict = attrib()
    steps: dict = attrib()

    @classmethod
    def create_audit(cls, model):
        """Create audit"""
        model_name = model.__class__.__name__
        arguments = _create_argument_output(model.arguments)
        arguments_summary = _create_argument_summary(arguments)
        output = _get_model_output(model)
        steps = _create_step_output(model.steps, output)
        steps_summary = _create_step_summary(steps)
        return cls(model_name, arguments_summary, arguments, steps_summary, steps)


# def create_signature_string(step):
#     """Create signature"""
#     sig = getfullargspec(step.function)
#     args = sig.args + sig.kwonlyargs
#     sig_str = ""
#     for idx, arg in enumerate(args, 1):
#         if arg in step.init_args:
#             sig_str += f"{arg}=argument({step.init_args.get(arg)})"
#         elif arg in step.dependent_args:
#             sig_str += f"{arg}=use({step.dependent_args.get(arg)})"
#         else:
#             sig_str += f"{arg}={step.defined_args.get(arg)}"
#         if idx < len(args):
#             sig_str += ", "
#         name = getattr(step.function, "name", None)
#         if name is None:
#             name = step.function.__name__
#     return f"{name}({sig_str})"

#########################################################################################
# to json
#########################################################################################

json_serializer = DispatchFunction("json_serializer", parameters=("dtype",))


PANDAS_JSON_KWARGS = {"orient": "records"}


@json_serializer.register(dtype=(pd.DataFrame, pd.Series))
def _(obj):
    return obj.to_dict(**PANDAS_JSON_KWARGS)


def json_serialize(obj):
    """Default json serializer"""
    return json_serializer(dtype=type(obj), obj=obj)


_STYLE_TITLE = NamedStyle(name="bold")
_STYLE_TITLE.font = Font(bold=True)
XLSX_FORMATS = {"title": _STYLE_TITLE}

#########################################################################################
# to xlsx
#########################################################################################


def create_xlsx_file(model_audit, file):
    """Create xlsx file."""
    wb = XlsxWorkbook.create()
    for format_nm, format_val in XLSX_FORMATS.items():
        wb.add_named_style(format_nm, format_val)

    # create worksheet
    model_name = model_audit.model_name
    wb.create_sheet(model_name, start_row=2, start_col=2)

    # write data
    arg_summary = pd.DataFrame.from_records(model_audit.arguments_summary)
    step_summary = pd.DataFrame.from_records(model_audit.steps_summary)
    wb.write_obj(model_name, model_audit.model_name)
    wb.write_obj(model_name, arg_summary, add_rows=1)
    wb.write_obj(model_name, step_summary, add_rows=1)
    # wb.write_obj(model_name, pd.DataFrame.from_records(model_audit.arguments))

    # format data
    wksht = wb.worksheets[model_name].obj
    wksht.sheet_view.showGridLines = False
    wksht.column_dimensions["A"].width = 2.14
    # wksht.set_row(1, None, wb.formats["title"])
    # wksht.set_column(0, 0, 2.14, wb.formats["title"])

    # write steps
    for step_name, step_value in model_audit.steps.items():

        # create worksheet
        wb.create_sheet(step_name, start_row=2, start_col=2)

        # write data
        wb.write_obj(step_name, {"Name:": step_name})
        wb.write_obj(
            step_name, {"Signature:": step_value["Signature"]},
        )
        wb.write_obj(
            step_name, {"Docstring:": step_value["Docstring"]},
        )
        wb.write_obj(step_name, {"Output:": step_value["Output"]})

        # format data
        wksht = wb.worksheets[step_name].obj
        wksht.sheet_view.showGridLines = False
        # wksht.set_row(1, None, wb.formats["title"])
        wksht.column_dimensions["A"].width = 2.14
        wksht.column_dimensions["B"].width = 12
        # wksht.set_column(0, 0, 2.14, wb.formats["title"])
        # wksht.set_column(1, 1, 12, wb.formats["title"])

    wb.save(file)


#########################################################################################
# run_model_audit
#########################################################################################

run_model_audit = DispatchFunction("run_model_audit", parameters=("output_type",))


@run_model_audit.register(output_type="json")
def _(model, file, **kwargs):
    """Run model audit"""
    audit = ModelAudit.create_audit(model)
    with open(file, "w") as stream:
        json.dump(obj=audit, fp=stream, default=json_serialize, **kwargs)


@run_model_audit.register(output_type="xlsx")
def _(model, file, **kwargs):
    """Run model audit"""
    audit = ModelAudit.create_audit(model)
    create_xlsx_file(audit, file, **kwargs)
