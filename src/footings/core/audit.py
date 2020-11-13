from copy import deepcopy

from attr import attrs, attrib
from numpydoc.docscrape import FunctionDoc
from openpyxl.styles import NamedStyle, Font

from .utils import dispatch_function
from .xlsx import FootingsXlsxWb
from ..doc_tools.docscrape import FootingsDoc


def _format_docstring(docstring):

    if docstring is None:
        return ""

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


def _get_model_output(model):
    output = {}
    for step in model.steps:
        func = getattr(model, step)
        func()
        results = deepcopy(model)
        step_output = {item: getattr(results, item) for item in func.impacts}
        if hasattr(model, step + "_audit"):
            getattr(results, step + "_audit")()
        output.update({step: step_output})

    return output


def _get_dtype(dtype):
    if dtype is None:
        return ""
    return dtype.__class__.__name__


def _create_step_output(model, output):
    """Create step output"""
    return_dict = {}
    for step in model.steps:
        step_dict = {}
        step_dict["Docstring"] = _format_docstring(getattr(model, step).__doc__)
        step_dict["Output"] = output[step]
        return_dict[step] = step_dict
    return return_dict


@attrs(slots=True, frozen=True, repr=False)
class ModelAudit:
    """Container for model audit output."""

    model_name: dict = attrib()
    model_doc: str = attrib()
    model_sig: str = attrib()
    steps: dict = attrib()

    @classmethod
    def create_audit(cls, model):
        """Create audit"""
        model_name = model.__class__.__name__
        model_doc = str(model.__doc__)
        model_sig = f"{model_name}{str(model.__signature__)}"
        output = _get_model_output(model)
        steps = _create_step_output(model, output)

        return cls(model_name, model_doc, model_sig, steps)


#########################################################################################
# to xlsx
#########################################################################################

XLSX_FORMATS = {
    "title": NamedStyle(name="bold", font=Font(name="Calibri", bold=True)),
    "underline": NamedStyle("underline", font=Font(name="Calibri", underline="single")),
    "hyperlink": NamedStyle(
        "hyperlink", font=Font(name="Calibri", italic=True, bold=True)
    ),
}


def create_xlsx_file(model_audit, file):
    """Create xlsx file."""
    wb = FootingsXlsxWb.create()
    for format_nm, format_val in XLSX_FORMATS.items():
        wb.add_named_style(format_nm, format_val)

    model_name = model_audit.model_name
    footings_headings = list(FootingsDoc.sections.keys()) + ["Steps"]
    function_headings = list(FunctionDoc.sections.keys())
    steps = list(model_audit.steps.keys())

    # create sheets
    wb.create_sheet(model_name, start_row=2, start_col=2)
    for step_name in steps:
        wb.create_sheet(step_name, start_row=2, start_col=2)

    # populate model sheet
    wb.write_obj(
        model_name, "Model Name:", add_cols=1, style=XLSX_FORMATS["title"], source="NAME"
    )
    wb.write_obj(model_name, model_name, add_rows=2, add_cols=-1, source="NAME")
    wb.write_obj(
        model_name,
        "Signature:",
        add_cols=1,
        style=XLSX_FORMATS["title"],
        source="SIGNATURE",
    )
    wb.write_obj(
        model_name, model_audit.model_sig, add_rows=2, add_cols=-1, source="SIGNATURE"
    )
    wb.write_obj(
        model_name,
        "Docstring:",
        add_cols=1,
        style=XLSX_FORMATS["title"],
        source="DOCSTRING",
    )

    in_steps_zone = False
    for line in model_audit.model_doc.split("\n"):
        if line in footings_headings:
            wb.write_obj(
                model_name,
                line,
                add_rows=1,
                style=XLSX_FORMATS["underline"],
                source="DOCSTRING",
            )
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
                source="DOCSTRING",
            )
        else:
            wb.write_obj(model_name, line, add_rows=1, source="DOCSTRING")

    # format model sheet
    wksht = wb.worksheets[model_name].obj
    wksht.sheet_view.showGridLines = False
    wksht.column_dimensions["A"].width = 2.14
    wksht.column_dimensions["B"].width = 14
    wksht.column_dimensions["C"].width = 1

    for step_name, step_value in model_audit.steps.items():

        # populate step sheets
        wb.write_obj(
            step_name,
            "Step Name:",
            add_cols=1,
            style=XLSX_FORMATS["title"],
            source="NAME",
        )
        wb.write_obj(step_name, step_name, add_rows=2, add_cols=-1, source="NAME")
        wb.write_obj(
            step_name,
            "Docstring:",
            add_cols=1,
            style=XLSX_FORMATS["title"],
            source="DOCSTRING",
        )

        step_docstring = (
            step_value["Docstring"] if step_value["Docstring"] is not None else ""
        )
        for line in step_docstring.split("\n"):
            if line in function_headings:
                wb.write_obj(
                    step_name,
                    line,
                    add_rows=1,
                    style=XLSX_FORMATS["underline"],
                    source="DOCSTRING",
                )
            elif "---" in line:
                pass
            else:
                wb.write_obj(step_name, line, add_rows=1)
        wb.write_obj(step_name, "", add_rows=1, add_cols=-1)
        wb.write_obj(
            step_name, "Output:", add_cols=2, style=XLSX_FORMATS["title"], source="OUTPUT"
        )
        wb.write_obj(step_name, step_value["Output"], source="OUTPUT")

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


@run_model_audit.register(output_type="xlsx")
def _(model, file, **kwargs):
    """Run model audit"""
    audit = ModelAudit.create_audit(model)
    create_xlsx_file(audit, file, **kwargs)
