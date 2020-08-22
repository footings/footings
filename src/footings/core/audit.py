import json
from copy import copy

from attr import attrs, attrib
import attr
import pandas as pd
from numpydoc.docscrape import FunctionDoc, ClassDoc
from openpyxl.styles import NamedStyle, Font

from .utils import dispatch_function
from .xlsx import FootingsXlsxWb


def _step_to_audit_format(step):
    """To audit format"""

    def _format_docstring(docstring):
        def _format_line(line, indent_len):
            if line[:indent_len] == "".join([" " for x in range(0, indent_len)]):
                return line[indent_len:]
            else:
                return line

        lines = docstring.split("\n")
        sections = [line for line in lines if "---" in line]
        if sections != []:
            section = sections[0]
            indent_len = len(section) - len(section.lstrip(" "))
        else:
            indent_len = 0
        return "\n".join(
            [_format_line(line, indent_len) if indent_len > 0 else line for line in lines]
        )

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
        if hasattr(v.function, "loaded"):
            init_params = {k: getattr(model, v) for k, v in v.init_params.items()}
            dependent_params = {
                k: copy(output[v.name]) for k, v in v.dependent_params.items()
            }
            output.update(
                {k: v.function(**init_params, **dependent_params, **v.defined_params)}
            )
        else:
            init_params = {k: getattr(model, v) for k, v in v.init_params.items()}
            dependent_params = {
                k: copy(output[v.name]) for k, v in v.dependent_params.items()
            }
            output.update(
                {k: v.function(**init_params, **dependent_params, **v.defined_params)}
            )
    return output


def _get_dtype(dtype):
    if dtype is None:
        return ""
    return dtype.__class__.__name__


def _create_parameter_output(parameters):
    """Create parameter output"""
    return {k: attr.asdict(v) for k, v in parameters.items()}


def _create_parameter_summary(parameters):
    return [
        {
            "Parameter": parameter["name"],
            "Type": _get_dtype(parameter["dtype"]),
            "Description": parameter["description"],
        }
        for parameter in parameters.values()
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
    model_doc: str = attrib()
    model_sig: str = attrib()
    parameters_summary: dict = attrib()
    parameters: dict = attrib()
    steps_summary: dict = attrib()
    steps: dict = attrib()

    @classmethod
    def create_audit(cls, model):
        """Create audit"""
        model_name = model.__class__.__name__
        model_doc = model.__doc__
        model_sig = f"{model_name}{str(model.__signature__)}"
        parameters = _create_parameter_output(model.parameters)
        parameters_summary = _create_parameter_summary(parameters)
        output = _get_model_output(model)
        steps = _create_step_output(model.steps, output)
        steps_summary = _create_step_summary(steps)
        return cls(
            model_name,
            model_doc,
            model_sig,
            parameters_summary,
            parameters,
            steps_summary,
            steps,
        )


#########################################################################################
# to json
#########################################################################################


@dispatch_function(key_parameters=("dtype",))
def json_serializer(dtype, obj):
    """ """
    msg = "No registered function based on passed paramters and no default function."
    raise NotImplementedError(msg)


PANDAS_JSON_KWARGS = {"orient": "records"}


@json_serializer.register(dtype=(pd.DataFrame, pd.Series))
def _(obj):
    return obj.to_dict(**PANDAS_JSON_KWARGS)


def json_serialize(obj):
    """Default json serializer"""
    return json_serializer(dtype=type(obj), obj=obj)


XLSX_FORMATS = {
    "title": NamedStyle(name="bold", font=Font(name="Calibri", bold=True)),
    "underline": NamedStyle("underline", font=Font(name="Calibri", underline="single")),
    "hyperlink": NamedStyle(
        "hyperlink", font=Font(name="Calibri", italic=True, bold=True)
    ),
}

#########################################################################################
# to xlsx
#########################################################################################


def create_xlsx_file(model_audit, file):
    """Create xlsx file."""
    wb = FootingsXlsxWb.create()
    for format_nm, format_val in XLSX_FORMATS.items():
        wb.add_named_style(format_nm, format_val)

    model_name = model_audit.model_name
    class_headings = list(ClassDoc.sections.keys()) + ["Steps"]
    function_headings = list(FunctionDoc.sections.keys())
    steps = list(model_audit.steps.keys())

    # create sheets
    wb.create_sheet(model_name, start_row=2, start_col=2)
    for step_name in steps:
        wb.create_sheet(step_name, start_row=2, start_col=2)

    # populate model sheet
    wb.write_obj(model_name, "Model Name:", add_cols=1, style=XLSX_FORMATS["title"])
    wb.write_obj(model_name, model_name, add_rows=2, add_cols=-1)
    wb.write_obj(model_name, "Signature:", add_cols=1, style=XLSX_FORMATS["title"])
    wb.write_obj(model_name, model_audit.model_sig, add_rows=2, add_cols=-1)
    wb.write_obj(model_name, "Docstring:", add_cols=1, style=XLSX_FORMATS["title"])

    in_steps_zone = False
    for line in model_audit.model_doc.split("\n"):
        if line in class_headings:
            wb.write_obj(model_name, line, add_rows=1, style=XLSX_FORMATS["underline"])
            if in_steps_zone:
                in_steps_zone = False
            if line == "Steps":
                in_steps_zone = True
        elif "---" in line:
            pass
        elif in_steps_zone and line in steps:
            wb.write_obj(
                model_name,
                line,
                add_rows=1,
                hyperlink=line,
                style=XLSX_FORMATS["hyperlink"],
            )
        else:
            wb.write_obj(model_name, line, add_rows=1)

    # format model sheet
    wksht = wb.worksheets[model_name].obj
    wksht.sheet_view.showGridLines = False
    wksht.column_dimensions["A"].width = 2.14
    wksht.column_dimensions["B"].width = 14
    wksht.column_dimensions["C"].width = 1

    for step_name, step_value in model_audit.steps.items():

        # populate step sheets
        wb.write_obj(step_name, "Step Name:", add_cols=1, style=XLSX_FORMATS["title"])
        wb.write_obj(step_name, step_name, add_rows=2, add_cols=-1)
        wb.write_obj(step_name, "Signature:", add_cols=1, style=XLSX_FORMATS["title"])
        wb.write_obj(step_name, step_value["Signature"], add_rows=2, add_cols=-1)
        wb.write_obj(step_name, "Docstring:", add_cols=1, style=XLSX_FORMATS["title"])

        for line in step_value["Docstring"].split("\n"):
            if line in function_headings:
                wb.write_obj(step_name, line, add_rows=1, style=XLSX_FORMATS["underline"])
            elif "---" in line:
                pass
            else:
                wb.write_obj(step_name, line, add_rows=1)
        wb.write_obj(step_name, "", add_rows=1, add_cols=-1)
        wb.write_obj(step_name, "Output:", add_cols=2, style=XLSX_FORMATS["title"])
        wb.write_obj(step_name, step_value["Output"])

        # format step sheets
        wksht = wb.worksheets[step_name].obj
        wksht.sheet_view.showGridLines = False
        wksht.column_dimensions["A"].width = 2.14
        wksht.column_dimensions["B"].width = 14
        wksht.column_dimensions["C"].width = 1
        for col in list(wksht.columns)[3:]:
            max_length = 0
            for cell in col:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            if max_length <= 3:
                adj_width = (max_length + 1) * 1.2
            else:
                adj_width = max_length + 3
            wksht.column_dimensions[col[0].column_letter].width = adj_width

    wb.save(file)


#########################################################################################
# run_model_audit
#########################################################################################


@dispatch_function(key_parameters=("output_type",))
def run_model_audit(model, file, **kwargs):
    """test run_model audit"""
    msg = "No registered function based on passed paramters and no default function."
    raise NotImplementedError(msg)


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
