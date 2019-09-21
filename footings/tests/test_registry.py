import pytest
import numpy as np

from footings import Column, CReturn, Frame, FReturn, Setting, Registry, as_calculation


class TestRegistry:
    def test_registry_using_columns(self):
        @as_calculation
        def calc_v(i: Column(float)) -> CReturn({"v": float}):
            return 1 / (1 + i)

        @as_calculation
        def calc_disc_factor(v: Column(float)) -> CReturn({"disc_factor": float}):
            return np.cumprod(v)

        @as_calculation
        def calc_disc_cash(
            cash: Column(float), disc_factor: Column(float)
        ) -> CReturn({"disc_cash": float}):
            return cash * disc_factor

        @as_calculation
        def calc_pv(disc_cash: Column(float)) -> CReturn({"pv": float}):
            return np.cumsum(disc_cash)

        reg1 = Registry(calc_v, calc_disc_factor, calc_disc_cash, calc_pv)
        assert all(
            i in ["v", "i", "disc_factor", "disc_cash", "cash", "pv"]
            for i in reg1.list_registered()
        )
        assert all(i in ["cash", "disc_factor"] for i in reg1.predecesors("disc_cash"))
        assert all(
            i in ["cash", "i", "v", "disc_factor"] for i in reg1.ancestors("disc_cash")
        )
        assert all(i in ["disc_cash"] for i in reg1.successors("disc_factor"))
        assert all(i in ["disc_cash", "pv"] for i in reg1.descendants("disc_factor"))

        reg2 = Registry()
        reg2.register(calc_v, calc_disc_factor, calc_disc_cash, calc_pv)
        assert all(
            i in ["v", "i", "disc_factor", "disc_cash", "cash", "pv"]
            for i in reg2.list_registered()
        )
