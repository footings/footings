"""Function and classes for auditing models."""

import json
from inspect import getfullargspec

from attr import attrs, attrib
import attr
import pandas as pd
import xlsxwriter

from footings.utils import LoadedFunction, DispatchFunction


def _get_model_output(model):
    output = {}
    for k, v in model.steps.items():
        if isinstance(v.function, LoadedFunction):
            init_args = {k: getattr(model, v) for k, v in v.init_args.items()}
            dependent_args = {k: output[v] for k, v in v.dependent_args.items()}
            output.update(
                {k: v.function(**init_args, **dependent_args, **v.defined_args)}
            )
        else:
            init_args = {k: getattr(model, v) for k, v in v.init_args.items()}
            dependent_args = {k: output[v] for k, v in v.dependent_args.items()}
            output.update(
                {k: v.function(**init_args, **dependent_args, **v.defined_args)}
            )
    return output


def create_signature_string(step):
    """Create signature"""
    sig = getfullargspec(step.function)
    args = sig.args + sig.kwonlyargs
    sig_str = ""
    for idx, arg in enumerate(args, 1):
        if arg in step.init_args:
            sig_str += f"{arg}=argument({step.init_args.get(arg)})"
        elif arg in step.dependent_args:
            sig_str += f"{arg}=use({step.dependent_args.get(arg)})"
        else:
            sig_str += f"{arg}={step.defined_args.get(arg)}"
        if idx < len(args):
            sig_str += ", "
        name = getattr(step.function, "name", None)
        if name is None:
            name = step.function.__name__
    return f"{name}({sig_str})"


def create_class_output(model_name, arguments, steps):
    """Create class output"""
    return_dict = {}
    return_dict["model_name"] = model_name

    def _get_dtype(dtype):
        if dtype is None:
            return ""
        return dtype.__class__.__name__

    return_dict["arguments"] = [
        {
            "Argument": argument["name"],
            "Type": _get_dtype(argument["dtype"]),
            "Description": argument["description"],
        }
        for argument in arguments.values()
    ]
    return_dict["steps"] = [
        {
            "Step": step["Name"],
            "Return Type": step["Output"].__class__.__name__,
            "Description": step["Description"],
        }
        for step in steps.values()
    ]
    return return_dict


def create_argument_output(arguments):
    """Create argument output"""
    return {k: attr.asdict(v) for k, v in arguments.items()}


def create_step_output(steps, steps_output):
    """Create step output"""
    return_dict = {}
    for key, value in steps.items():
        step_dict = {}
        step_dict["Name"] = key
        step_dict["Description"] = value.__doc__
        step_dict["Signature"] = value.create_signature_string()
        step_dict["Meta"] = value.meta
        step_dict["Output"] = steps_output.get(key)
        return_dict[key] = step_dict
    return return_dict


def create_return_dict(model):
    """Create return dict"""
    step_output = _get_model_output(model)
    return_dict = {}
    return_dict["arguments"] = create_argument_output(model.arguments)
    return_dict["steps"] = create_step_output(model.steps, step_output)
    return_dict["model"] = create_class_output(
        model.__class__.__name__, return_dict["arguments"], return_dict["steps"]
    )
    return return_dict


#########################################################################################
# json_serializer
#########################################################################################

json_serializer = DispatchFunction("json_serializer", parameters=("dtype",))


PANDAS_JSON_KWARGS = {"orient": "records"}


@json_serializer.register(dtype=(pd.DataFrame, pd.Series))
def _(obj):
    return obj.to_dict(**PANDAS_JSON_KWARGS)


def json_serialize(obj):
    """Default json serializer"""
    return json_serializer(dtype=type(obj), obj=obj)


#########################################################################################
# xlsx_dispatch
#########################################################################################


@attrs(slots=True, frozen=True, repr=False)
class XlsxRange:
    """XlsxRange"""

    row_start: int = attrib()
    col_start: int = attrib()
    row_end: int = attrib()
    col_end: int = attrib()


def _xlsx_dispatch_default(worksheet, obj, start_row, start_col, **kwargs):
    worksheet.write(start_row, start_col, obj, **kwargs)
    end_row = start_row
    end_col = start_col
    return XlsxRange(start_row, start_col, end_row, end_col)


xlsx_dispatch = DispatchFunction(
    "xlsx_dispatch", parameters=("dtype",), default=_xlsx_dispatch_default
)


@xlsx_dispatch.register(dtype=type)
def _(worksheet, obj, start_row, start_col, **kwargs):
    worksheet.write(start_row, start_col, str(obj), **kwargs)
    end_row = start_row
    end_col = start_col
    return XlsxRange(start_row, start_col, end_row, end_col)


@xlsx_dispatch.register(dtype=(list, tuple, range))
def _(worksheet, obj, start_row, start_col, **kwargs):
    for x in obj:
        ret_xlsx = obj_to_excel(worksheet, x, start_row, start_col, **kwargs)
    end_row = ret_xlsx.row_end
    end_col = ret_xlsx.col_end
    return XlsxRange(start_row, start_col, end_row, end_col)


@xlsx_dispatch.register(dtype=dict)
def _(worksheet, obj, start_row, start_col, **kwargs):

    if len(obj) > 0:
        for k, v in obj.items():
            obj_to_excel(worksheet, k, start_row, start_col, **kwargs)
            ret_xlsx = obj_to_excel(worksheet, v, start_row, start_col + 1, **kwargs)
        end_row = ret_xlsx.row_end
        end_col = ret_xlsx.col_end
    else:
        end_row = start_row
        end_col = start_col
    return XlsxRange(start_row, start_col, end_row, end_col)


PANDAS_HEADER_FORMAT = {}


@xlsx_dispatch.register(dtype=(pd.DataFrame, pd.Series))
def _(worksheet, obj, start_row, start_col, **kwargs):
    # write header
    worksheet.write_row(start_row, start_col, obj.columns, **kwargs)
    # write rows
    for _, row in obj.iterrows():
        start_row += 1
        worksheet.write_row(start_row, start_col, row, **kwargs)
    end_row = start_row + obj.shape[0] - 1
    end_col = start_col + obj.shape[1] - 1
    return XlsxRange(start_row, start_col, end_row, end_col)


def obj_to_excel(worksheet, obj, start_row, start_col, **kwargs):
    """Default xlsx serializer"""
    return xlsx_dispatch(
        dtype=type(obj),
        worksheet=worksheet,
        start_row=start_row,
        start_col=start_col,
        obj=obj,
        **kwargs,
    )


#########################################################################################
# to_excel
#########################################################################################


def write_model_doc(workbook, class_info):
    """Write model documentation"""
    wrkst = workbook.add_worksheet("model")
    wrkst.hide_gridlines(2)

    start_row = 1
    start_col = 1

    ret_range_1 = obj_to_excel(wrkst, class_info["model_name"], start_row, start_col)

    arguments = pd.DataFrame.from_records(class_info["arguments"])
    ret_range_2 = obj_to_excel(wrkst, arguments, ret_range_1.row_end + 2, start_col)

    steps = pd.DataFrame.from_records(class_info["steps"])
    obj_to_excel(wrkst, steps, ret_range_2.row_end + 2, start_col)

    # format column widths
    wrkst.set_column(0, 0, width=2.14)


def write_arugments(workbook, arguments):
    """Write arguments"""
    wrkst = workbook.add_worksheet("arguments")
    wrkst.hide_gridlines(2)
    wrkst.set_column(0, 0, width=2.14)
    start_row = 1
    start_col = 1
    obj_to_excel(wrkst, arguments, start_row, start_col)


def write_steps(workbook, steps):
    """Write steps"""
    for step_name, step_value in steps.items():
        wrkst = workbook.add_worksheet(step_name)
        wrkst.hide_gridlines(2)
        wrkst.set_column(0, 0, width=2.14)
        obj_to_excel(wrkst, {"Name:": step_value["Name"]}, 1, 1)
        obj_to_excel(wrkst, {"Description:": step_value["Description"]}, 2, 1)
        obj_to_excel(wrkst, {"Signature:": step_value["Signature"]}, 3, 1)
        obj_to_excel(wrkst, {"Meta:": step_value["Meta"]}, 4, 1)
        obj_to_excel(wrkst, step_value["Output"], 6, 1)


def to_excel(dict_, file):
    """Create model audit file in excel."""

    workbook = xlsxwriter.Workbook(file, {"nan_inf_to_errors": True})
    write_model_doc(workbook, dict_["model"])
    # write_arugments(workbook, dict_["arguments"])
    write_steps(workbook, dict_["steps"])
    workbook.close()


#########################################################################################
# run_model_audit
#########################################################################################

run_model_audit = DispatchFunction("run_model_audit", parameters=("output_type",))


@run_model_audit.register(output_type="json")
def _(model, file, **kwargs):
    """Run model audit"""
    ret_dict = create_return_dict(model)
    with open(file, "w") as stream:
        json.dump(obj=ret_dict, fp=stream, default=json_serialize, **kwargs)


@run_model_audit.register(output_type="xlsx")
def _(model, file, **kwargs):
    """Run model audit"""
    ret_dict = create_return_dict(model)
    to_excel(ret_dict, file, **kwargs)
