
from numpy import cumprod, cumsum
import pandas as pd
import dask.dataframe as dd

from footings import FootingsModel, Registry, Calculation, as_calculation, Column, CReturn, Setting
from footings.model import _build_model_graph, _get_functions, FootingsModel

class TestModel:
    """
    """
    def test_model(self):

        df = pd.DataFrame({
            't': [0, 1, 2, 3],
            'i': [0, 0.1, 0.09, 0.08], 
            'cash': [1000, -350, -350, -350]})
        df.set_index('t', inplace=True)
        ddf = dd.from_pandas(df, npartitions=1)

        @as_calculation
        def calc_v(i: Column(float)) -> CReturn({'v': float}):
            return 1 / (1 + i)
        
        @as_calculation
        def calc_disc_factor(v: Column(float)) -> CReturn({'disc_factor': float}):
            return cumprod(v)
        
        @as_calculation
        def calc_disc_cash(
            cash: Column(float), 
            disc_factor: Column(float)) -> CReturn({'disc_cash': float}):
            return cash * disc_factor
        
        @as_calculation
        def calc_pv(disc_cash: Column(float)) -> CReturn({'pv': float}):
            return cumsum(disc_cash)

        reg = Registry(calc_v, calc_disc_factor, calc_disc_cash, calc_pv)
        # z = _build_model_graph(df, reg, None)
        # _get_functions()
        # m = FootingsModel(ddf, reg)
