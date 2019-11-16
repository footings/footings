import pandas as pd
import dask.dataframe as dd
from pyarrow import Schema
from dask.base import DaskMethodsMixin
from networkx import topological_sort
from functools import partial

from .annotation import Setting, Column, CReturn, Frame, FReturn

from .utils import _generate_message
from .function import _BaseFunction


class DaskComponents:
    """
    """

    def compute(self, **kwargs):
        return self._frame.compute(**kwargs)

    def persist(self, **kwargs):
        return self._frame.persist(**kwargs)

    def visualize_graph(
        self, filename="mydask", format=None, optimize_graph=False, **kwargs
    ):
        return self.frame.visualize(
            filename=filename, format=format, optimize_graph=optimize_graph, **kwargs
        )

    def visualize_frame(
        self, filename="mydask", format=None, optimize_graph=False, **kwargs
    ):
        return self.frame.visualize(
            filename=filename, format=format, optimize_graph=optimize_graph, **kwargs
        )


class ModelDescription:
    """
    """

    def __init__(self, frame, tempalte):
        pass


class ModelTemplate:
    """
    """

    def __init__(
        self, registry, runtime_settings=None, scenario=None, step=True, **kwargs
    ):
        self.registry = registry
        self._frame = self.registry.get_primary_frame()
        self.runtime_settings = self._get_runtime_settings(runtime_settings)
        # self.defined_settings = self._get_defined_settings(**kwargs)
        self.scenario = self._validate_scenario(scenario)
        self.step = step
        # self.instructions = self._build_instructions()
        self._runtime_checks = self._create_runtime_check()
        # self._dask_functions = self._build_dask_function()
        # self._dask_meta = self._build_dask_meta()

    def _get_runtime_settings(self, runtime_settings):
        """[summary]
        
        Parameters
        ----------
        runtime_settings : [type]
            [description]
        
        Returns
        -------
        [type]
            [description]
        """
        if runtime_settings is None:
            return None
        else:
            settings = self.registry.get_settings()
            return {k: v["setting"] for k, v in settings.items() if k in runtime_settings}

    def _get_defined_settings(self, **kwargs):
        # still need to consider kwargs
        settings = self._model_graph.settings
        runtime = self.runtime_settings
        if settings is not None and runtime is not None:
            defined = {k: v for k, v in settings.items() if k not in runtime.keys()}
        elif settings is not None and runtime is None:
            defined = settings
        else:
            defined = None
        if defined is not None and kwargs is not None:
            return kwargs
        else:
            return None

    def _validate_scenario(self, scenario):
        return scenario

    def _build_instructions(self):
        instr = {"frame": {"columns": self._model_graph.frame_meta.columns}}
        output_list = []
        for k, v in self._model_graph.functions.items():
            output = _parse_annotation_output(v["src"])
            output_list.append(output)
            settings = _parse_annotation_settings(v["src"])
            if settings != {}:
                if self.runtime_settings is not None:
                    runtime = {
                        k: None for k, v in self.runtime_settings.items() if k in settings
                    }
                else:
                    runtime = None
                if self.defined_settings is not None:
                    defined = {
                        k: v for k, v in self.defined_settings.items() if k in settings
                    }
                else:
                    defined = None
            else:
                runtime = None
                defined = None
            instr.update(
                {
                    k: {
                        "ftype": v["ftype"],
                        "src": v["src"],
                        "runtime_settings": runtime,
                        "defined_settings": defined,
                        "input_columns": _parse_annotation_input(v["src"]),
                        "output_columns": output,
                        "drop_columns": [],
                    }
                }
            )
        return instr

    def _build_dask_function(self):
        func_list = [
            (
                ff_function(
                    v["src"],
                    v["runtime_settings"],
                    v["defined_settings"],
                    v["input_columns"],
                    v["output_columns"],
                ),
                v["runtime_settings"],
            )
            for k, v in self.instructions.items()
            if "src" in v
        ]

        if self.step is True:
            return func_list
        else:
            # def _combine_functions(func_list):
            #    def func_model(_df):
            #        # need to incorporate to_ff_function
            #        for f in func_list:
            #            _df = f(_df)
            #        return _df
            #    return func_model
            return None  # _combine_functions(func_list)

    def _create_runtime_check(self):
        pass

    def _build_dask_meta(self):
        frame = {i: str(v) for i, v in self.frame_meta.dtypes.iteritems()}
        change = {
            c: d
            for k, v in self.instructions.items()
            if "output_columns" in v
            for c, d in v["output_columns"].items()
        }
        d = []
        for k, v in change.items():
            if d == []:
                d.append({**frame, k: v})
            else:
                d.append({**d[-1], k: v})
        return d

    def visualize_graph(self):
        pass

    def visualize_frame(self):
        pass

    def sub(self):
        pass

    def __call__(self, frame, **kwargs):
        return ModelFromTemplate(frame=frame, template=self, **kwargs)


def as_model_template():
    pass


class ModelFromTemplate(DaskComponents):
    """
    
    """

    def __init__(self, frame, template, **kwargs):
        self.template = template
        self._run_runtime_checks()
        self._frame = self._run_model(frame, **kwargs)
        # self.description = ModelDescription(frame, template)

    def _run_runtime_checks(self):
        if self.template._runtime_checks is not None:
            pass

    def _run_model(self, frame, **kwargs):
        meta = self.template._dask_meta
        functions = self.template._dask_functions
        ddf = frame.copy()

        for f, m in zip(functions, meta):
            if f[1] is not None:
                kws = {k: v for k, v in kwargs.items() if k in f[1]}
                ddf = ddf.map_partitions(f[0], meta=m, **kws)
            else:
                ddf = ddf.map_partitions(f[0], meta=m)
        return ddf

    def sub(self, x, y):
        pass

    def audit(self):
        pass

    def reduce_func(self):
        pass


class Model(ModelFromTemplate):
    """
    """

    def __init__(self, **kwargs):
        temp_kws = ""
        non_temp_kws = ""
        super().__init__(template=ModelTemplate(**kwargs))
