from footings import Calculation, as_calculation, Column, CReturn


class TestCalculation:
    def test_calculation(self):
        def calc_v(i: Column(float)) -> CReturn({"v": float}):
            return 1 / (1 + i)

        v = Calculation(calc_v)
        assert isinstance(v, Calculation)
        assert v(1) == 0.5
