"""Shared objects to use across testing"""

import pandas as pd
from footings import create_argument, use


def step_1(a, add):
    """
    Run step_1.

    Parameters
    ----------
    a : int
        A number
    add : int
        Amount to add

    Returns
    -------
    int
    """
    return a + add


def step_2(b, subtract):
    """
    Run step_2.

    Parameters
    ----------
    b : int
        A number
    subtract : int
        Amount to subtract

    Returns
    -------
    int
    """
    return b - subtract


def step_3(a, b, c):
    """
    Run step_3.

    Parameters
    ----------
    a : int
        A number A
    b : int
        A number B
    c : int
        A number C

    Returns
    -------
    int
    """
    return a + b + c


def create_dict(a, b):
    """
    Create dict

    Parameters
    ----------
    a : int
        A number A
    b : int
        A number B

    Returns
    -------
    int
    """
    return {"a": a, "b": b}


def create_frame(n):
    """
    Create DataFrame with rows equal to n.

    Parameters
    ----------
    n : int
        The number of rows to the frame

    Returns
    -------
    pd.DataFrame
    """
    return pd.DataFrame({"n": range(0, n)})


def frame_add_column(frame, add):
    """
    Create add column in frame

    Parameters
    ----------
    frame : pd.DataFrame
        The dataframe passed
    add : int
        The amount to add

    Returns
    -------
    pd.DataFrame
    """
    return frame.assign(add_col=lambda df: df["n"] + add)


def frame_subtract_column(frame, subtract):
    """
    Create subtract column in frame

    Parameters
    ----------
    frame : pd.DataFrame
        The dataframe passed
    subtract : int
        The amount to subtract

    Returns
    -------
    pd.DataFrame
    """
    return frame.assign(subtract_col=lambda df: df["n"] - subtract)


def return_x(x):
    """Return frame"""
    return x


STEPS_USING_INTEGERS = [
    {
        "name": "step_1",
        "function": step_1,
        "args": {"a": create_argument("a", description="description for a"), "add": 1,},
    },
    {
        "name": "step_2",
        "function": step_2,
        "args": {
            "b": create_argument("b", description="description for b"),
            "subtract": 1,
        },
    },
    {
        "name": "step_3",
        "function": step_3,
        "args": {
            "a": use("step_1"),
            "b": use("step_2"),
            "c": create_argument("c", description="description for c"),
        },
    },
]


STEPS_USING_PANDAS = [
    {
        "name": "create_frame",
        "function": create_frame,
        "args": {"n": create_argument("n", description="N rows of frame.")},
    },
    {
        "name": "frame_add_column",
        "function": frame_add_column,
        "args": {
            "frame": use("create_frame"),
            "add": create_argument("add", description="Amount to add."),
        },
    },
    {
        "name": "frame_subtract_column",
        "function": frame_subtract_column,
        "args": {
            "frame": use("frame_add_column"),
            "subtract": create_argument("subtract", description="Amount to subtract."),
        },
    },
]


STEPS_USING_ATTR_LOOKUP = STEPS_USING_PANDAS.copy()
STEPS_USING_ATTR_LOOKUP.append(
    {
        "name": "lookup_attr",
        "function": return_x,
        "args": {"x": use("frame_subtract_column", get_attr="n"),},
    }
)


STEPS_USING_KEY_LOOKUP = [
    {
        "name": "create_dict",
        "function": create_dict,
        "args": {
            "a": create_argument("a", description="description for a"),
            "b": create_argument("b", description="description for b"),
        },
    },
    {
        "name": "return_a",
        "function": return_x,
        "args": {"x": use("create_dict", get_key="a")},
    },
]
