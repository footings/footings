import pandas as pd
from inspect import getfullargspec
from functools import wraps, partial

from .annotation import Column, CReturn, Frame, FReturn, Setting

from .utils import _generate_message


def func_annotation_valid(function):
    # validate all args annotated
    args = set(getfullargspec(function).args)
    anno = set(function.__annotations__.keys())
    diff_args = args.difference(anno)
    if len(diff_args) > 0:
        msg1 = "The following function parameters need to be annotated - "
        raise AssertionError(_generate_message(msg1, diff_args))

    # validate input annotated types
    unk_types = []
    not_inste = []
    for k, v in function.__annotations__.items():
        if k != "return":
            if (
                v is not Column
                and v is not Frame
                and v is not Setting
                and isinstance(v, type)
            ):
                unk_types.append(k)
            if (
                isinstance(v, Column) == False
                and isinstance(v, Frame) == False
                and isinstance(v, Setting) == False
            ):
                not_inste.append(k)

    if len(unk_types) > 0:
        msg2 = "The following function parameters are assigned invalid input types - "
        raise AssertionError(_generate_message(msg2, unk_types))

    if len(not_inste) > 0:
        msg3 = "The following function parameters are assigned non instantiated types - "
        raise AssertionError(_generate_message(msg3, not_inste))

    # validate input combinations
    items = function.__annotations__.items()
    c = [v for k, v in items if k != "return" and isinstance(v, Column)]
    f = [v for k, v in items if k != "return" and isinstance(v, Frame)]
    assert (
        len(c) > 0 and len(f) > 0
    ) == False, "Column and Frame types cannot be used together as annotation inputs."

    # assert len(f) < 2, "Mutiple Frames cannot be used as input"

    # validate function return is annotated
    assert "return" in function.__annotations__, "Function return needs to be annotated"

    # validate input/return combinations
    if len(c) > 0 and isinstance(function.__annotations__["return"], CReturn) == False:
        msg4 = (
            "If using Columns as input, return needs to be annotated with "
            "an instance of CReturn."
        )
        raise AssertionError(msg4)

    if len(f) > 0 and isinstance(function.__annotations__["return"], FReturn) == False:
        msg5 = (
            "If using a Frame as input, return needs to be annotated with "
            "an instance of FReturn."
        )
        raise AssertionError(msg5)

    return True


def _get_column_inputs(function):
    l = []
    for k, v in function.__annotations__.items():
        if k != "return" and type(v) == Column:
            l.append((k, {"class": Column, "dtype": v.dtype}))
        elif k != "return" and type(v) == Frame:
            for c, t in v.columns.items():
                l.append((c, {"class": Column, "dtype": t}))
    return l


def _get_setting_inputs(function):
    l = []
    for k, v in function.__annotations__.items():
        if k != "return" and type(v) == Setting:
            l.append(
                (
                    k,
                    {
                        "class": Setting,
                        "dtype": v.dtype,
                        "default": v.default,
                        "allowed": v.allowed,
                    },
                )
            )
    return l


def _get_column_ouputs(function, output_attrs=None):
    l = []
    for k, v in function.__annotations__.items():
        if k == "return" and output_attrs is None:
            for c, t in v.columns.items():
                l.append((c, {"src": function, "class": Column, "dtype": t}))
        elif k == "return" and output_attrs is not None:
            for c, t in v.columns.items():
                l.append(
                    (c, {"src": function, "class": Column, "dtype": t, **output_attrs})
                )
    return l


class _BaseFunction:
    """
    A class to identify functions specifically built for the footings framework.
    """

    def __init__(self, function, output_attrs=None):

        func_annotation_valid(function)

        # ff_function = to_ff_function(function)
        self.annotations = function.__annotations__
        self.function = function
        # self.ff_function = ff_function
        self.name = function.__name__
        self.columns_input = _get_column_inputs(function)
        self.setting_input = _get_setting_inputs(function)
        self.columns_output = _get_column_ouputs(function, output_attrs)

    def __call__(self, *args, **kwargs):
        return self.function(*args, **kwargs)

    # def __repr__(self):
    #     pass
