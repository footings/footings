import pandas as pd
import dask.dataframe as dd
from pyarrow import Schema
from dask.base import DaskMethodsMixin
from networkx import topological_sort
from functools import partial

from .annotation import Setting, Column, CReturn, Frame, FReturn
from .registry import Registry
from .utils import _generate_message
from .function import _BaseFunction


class DaskComponents:
    """
    """

    def compute(self, **kwargs):
        return self._modeled_frame.compute(**kwargs)

    def persist(self, **kwargs):
        return self._modeled_frame.persist(**kwargs)

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
        self,
        registry: Registry,
        runtime_settings: list = None,
        defined_settings: dict = None,
        scenarios: list = None,
        step: bool = True,
        model_meta: dict = None,
    ):
        self._model_meta = model_meta
        self._registry = registry
        self._settings = self._registry.get_settings()
        self._runtime_settings = self._get_runtime_settings(runtime_settings)
        self._defined_settings = self._get_defined_settings(defined_settings)
        self._scenarios = self._validate_scenarios(scenarios)
        self._instructions = self._build_instructions()
        self._runtime_checks = self._create_runtime_check()
        self._step = step
        self._dask_instructions = self._build_dask_instructions()

    @property
    def model_meta(self):
        return self._model_meta

    @property
    def registry(self):
        return self._registry

    @property
    def starting_frame_meta(self):
        return self._registry._starting_frame_meta

    @property
    def runtime_settings(self):
        return self._runtime_settings

    @property
    def defined_settings(self):
        return self._defined_settings

    @property
    def scenarios(self):
        return self._scenarios

    @property
    def instructions(self):
        return self._instructions

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

    def _get_defined_settings(self, defined_settings):
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
        if self._settings == {} and defined_settings is None:
            return None
        elif self._settings == {} and defined_settings is not None:
            raise (
                AssertionError,
                "No settings defined in registry, but defined settings passed to template.",
            )
        elif self._settings != {} and defined_settings is None:
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
            for k, v in defined_settings.items():
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

    def _validate_scenarios(self, scenarios):
        return scenarios

    def _build_instructions(self):
        """[summary]
        
        Returns
        -------
        [type]
            [description]
        """
        functions = self._registry.get_ordered_functions()
        instr = {"starting_frame": self.starting_frame_meta, "functions": {}}

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
                    defined = {k: v for k, v in self._defined_settings.items() if k in l}
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

    def _create_runtime_check(self):
        pass

    def _build_dask_instructions(self):
        if self._step is True:
            func_list = []
            meta = self.starting_frame_meta.copy()
            for k, v in self._instructions["functions"].items():
                meta.update(v["output_columns"])
                func_list.append(
                    {
                        "function": v["src"]._ff_function,
                        "defined_settings": v["defined_settings"],
                        "runtime_settings": v["runtime_settings"],
                        "meta": meta.copy(),
                    }
                )
            # print(func_list)
            return func_list

        elif self._step is False:
            # need to develop logic to bundle function calls to assist dask
            raise (AssertionError, "Not implemented")

        else:
            raise (AssertionError, "step needs to be True or False")

    def visualize_graph(self):
        pass

    def visualize_frame(self):
        pass

    def sub(self):
        pass

    def __call__(self, starting_frame, **kwargs):
        return ModelFromTemplate(starting_frame=starting_frame, template=self, **kwargs)


class ModelFromTemplate(DaskComponents):
    """
    
    """

    def __init__(self, starting_frame: dd.DataFrame, template: ModelTemplate, **kwargs):
        self._template = template
        self._run_runtime_checks(starting_frame, **kwargs)
        self._modeled_frame = self._run_model(starting_frame, **kwargs)
        # self.description = ModelDescription(frame, template)

    def _run_runtime_checks(self, starting_frame, **kwargs):
        pass
        # if self._template._runtime_checks is not None:
        #     pass

    def _run_model(self, starting_frame, **kwargs):
        ddf = starting_frame.copy()
        for f in self._template._dask_instructions:
            print(f)
            kws = {}
            if f["defined_settings"] is not None:
                kws.update(f["defined_settings"])
            if f["runtime_settings"] is not None:
                kws.update(
                    {k: v for k, v in kwargs.items() if k in f["runtime_settings"]}
                )
            if kws != {}:
                ddf = ddf.map_partitions(f["function"], meta=f["meta"], **kws)
            else:
                ddf = ddf.map_partitions(f["function"], meta=f["meta"])
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

    def __init__(self, starting_frame, **kwargs):
        temp_kws = ""
        non_temp_kws = ""
        super().__init__(starting_frame=starting_frame, template=ModelTemplate(**kwargs))
