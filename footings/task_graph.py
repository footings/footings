"""task_graph.py"""

from inspect import getfullargspec

from dask.dataframe import map_partitions
from toolz import curry

from .parameter import Parameter
from .utils import PriorStep, GetTbl


def _split_args(args, levels, table_name):
    """Split args into prior args, paramater args, and partial args."""
    prior_args = {}
    param_args = {}
    partial_args = {}
    for k, v in args.items():
        if isinstance(v, GetTbl):
            prior_args.update({k: table_name})
        elif isinstance(v, PriorStep):
            prior_args.update({k: v.get_name(levels)})
        elif isinstance(v, Parameter):
            param_args.update({k: v.name})
        else:
            partial_args.update({k: v})
    return prior_args, param_args, partial_args


def _update_meta(meta, add=None, modify=None, remove=None, returned=None):
    if returned is not None:
        return [(column.name, column.dtype) for column in returned]
    if add is not None:
        for column in add:
            meta.append((column.name, column.dtype))
    if modify is not None:
        raise NotImplementedError()
    if remove is not None:
        remove_nms = [column.name for column in remove]
        meta = [column for column in meta if column[0] not in remove_nms]
    return meta


def _to_dd_task_graph(plan):
    """Creates dask dataframe task graph"""
    partitioned = False
    params = {}
    tasks = {}
    meta = [(column.name, column.dtype) for column in plan.tbl.columns]
    for idx, level in enumerate(plan.levels):
        meta = _update_meta(
            meta,
            level.added_columns,
            level.modified_columns,
            level.removed_columns,
            level.returned_columns,
        )
        if level.collapse is True:
            partitioned = False
        prior_args, param_args, partial_args = _split_args(
            level.args, plan.levels[:idx], plan.tbl.name
        )
        task_args = {**prior_args, **param_args}
        params.update({v: "UNDEFINED" for v in param_args.values()})
        arg_position = getfullargspec(level.task).args
        if partial_args == {}:
            func = level.task
        else:
            func = curry(level.task, **partial_args)
        if partitioned is False:
            task = (func, *[task_args.get(arg) for arg in arg_position])
        else:
            # map_part_meta = curry(map_partitions, meta=meta)
            task = (map_partitions, func, *[task_args.get(arg) for arg in arg_position])
        tasks.update({level.name: task})
        if level.partition is True:
            partitioned = True
    return {plan.tbl.name: "UNDEFINED", **params, **tasks}


class TaskGraphMethodNotDefined(Exception):
    """Task graph method is not defined"""


_TASK_GRAPH_OPTIONS = {"dd": _to_dd_task_graph}


def to_task_graph(plan, method):
    """Dispatch task graph function"""
    func = _TASK_GRAPH_OPTIONS.get(method, None)
    if func is None:
        raise TaskGraphMethodNotDefined(
            f"The method [{method}] is not defined as a task graph option."
        )
    return func(plan)


def create_graph(model):
    """Create graph from model"""
    return model
