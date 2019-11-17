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
        self, registry, runtime_settings=None, scenarios=None, step=True, **kwargs
    ):
        self._registry = registry
        if self._registry.frame_metas is not {}:
            for k, v in self._registry.frame_metas.items():
                assert hasattr(self, "_{0}".format(k)) == False
                setattr(self, "_{0}".format(k), v)
                self._primary = k
        self._settings = self._registry.get_settings()
        self._runtime_settings = self._get_runtime_settings(runtime_settings)
        self._defined_settings = self._get_defined_settings(**kwargs)
        self._scenarios = self._validate_scenarios(scenarios)
        self._instructions = self._build_instructions()
        self._runtime_checks = self._create_runtime_check()
        self._step = step
        self._dask_functions = self._build_dask_function()
        self._dask_meta = self._build_dask_meta()

    @property
    def registry(self):
        return self._registry

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
        elif self._settings == {}:
            raise (
                AssertionError,
                "There are no settings in the registry yet a runtime setting is set.",
            )
        else:
            assert all(
                [s in self._settings.keys() for s in runtime_settings]
            ), "Not all runtime_settings are present in settings"
            return {
                k: v["setting"]
                for k, v in self._settings.items()
                if k in runtime_settings
            }

    @property
    def runtime_settings(self):
        return self._runtime_settings

    def _get_defined_settings(self, **kwargs):
        """[summary]
        
        Parameters
        ----------
        kwargs : [type]
            [description]

        Returns
        -------
        [type]
            [description]
        """
        if self._settings is None:
            return None
        else:
            if self._runtime_settings is None:
                settings = self._settings.copy()
            else:
                settings = {
                    k: v
                    for k, v in self._settings.items()
                    if k not in self._runtime_settings
                }

            defined = {}
            # 1. get settings defined in kwargs and validate
            for k, v in kwargs.items():
                if k in settings:
                    assert settings[k]["setting"].valid(
                        v
                    ), "{0} is not a valid value for the setting {1}".format(v, k)
                    defined.update({k: v})
                    del settings[k]

            # 2. any left over from 1 need to check to see if default exist
            for k, v in settings.items():
                if v["setting"].default is not None:
                    defined.update({k: v})
                    del settings[k]

            # 3. any left over from 2 needs to raise an error
            if len(settings) > 0:
                msg = "The following settings are not set at runtime or defined in kwargs and do not have a default "
                raise (AssertionError, _generate_message(msg, settings.keys()))

            return defined

    @property
    def defined_settings(self):
        return self._defined_settings

    def _validate_scenarios(self, scenarios):
        return scenarios

    @property
    def scenarios(self):
        return self._scenarios

    def _build_instructions(self):
        """[summary]
        
        Returns
        -------
        [type]
            [description]
        """
        functions = self._registry.get_ordered_functions()
        instr = {
            "frames": {
                self._primary: (
                    self._registry._frame_metas[self._primary]
                    .reset_index()
                    .dtypes.to_dict()
                )
            },
            "functions": {},
        }

        for f in functions:
            runtime = None
            defined = None
            if f.settings != {}:
                if self._runtime_settings is not None:
                    runtime = [
                        k for k in f.settings.keys() if k in self._runtime_settings
                    ]
                if self._defined_settings is not None:
                    l = [k for k in f.settings.keys() if k in self._defined_settings]
                    defined = {k: v for k, v in self._defined_settings if k in l}
            instr["functions"].update(
                {
                    f.name: {
                        "ftype": type(f),
                        "src": f,
                        "runtime_settings": runtime if runtime != [] else None,
                        "defined_settings": defined if defined != {} else None,
                        "input_columns": f.input_columns,
                        "output_columns": f.output_columns,
                        "drop_columns": [],
                    }
                }
            )

        return instr

    @property
    def instructions(self):
        return self._instructions

    def _build_dask_function(self):
        func_list = [
            (v["src"]._ff_function, v["defined_settings"], v["runtime_settings"])
            for k, v in self._instructions["functions"].items()
            if "src" in v
        ]
        if self._step is True:
            return func_list
        elif self._step is False:
            # need to develop logic to bundle function calls to assist dask
            raise (AssertionError, "Not implemented")
        else:
            raise (AssertionError, "step needs to be True or False")

    def _create_runtime_check(self):
        pass

    def _build_dask_meta(self):
        frame = self._instructions["frames"][self._primary]
        change = {
            c: d
            for k, v in self._instructions["functions"].items()
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

    def __init__(self, template, **kwargs):
        self._template = template
        self._run_runtime_checks()
        self._frame = self._run_model(**kwargs)
        # self.description = ModelDescription(frame, template)

    def _run_runtime_checks(self):
        if self._template._runtime_checks is not None:
            pass

    def _run_model(self, **kwargs):
        meta = self._template._dask_meta
        functions = self._template._dask_functions
        assert self._template._primary in kwargs, "primary frame not in kwargs"
        ddf = kwargs[self._template._primary].copy()

        for f, m in zip(functions, meta):
            kws = {}
            if f[1] is not None:
                kws.update(f[1])
            if f[2] is not None:
                kws.update({k: v for k, v in kwargs.items() if k in f[2]})
            if kws != {}:
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
