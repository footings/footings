from footings import Calculation, as_calculation, Column, CReturn


def test_calculation():
    def calc_v(i: Column("float")) -> CReturn({"v": "float"}):
        return 1 / (1 + i)

    v = Calculation(calc_v, method="A")
    assert isinstance(v, Calculation)
    assert v(1) == 0.5


def test_as_calculation():
    @as_calculation(method="A")
    def calc_v(i: Column("float")) -> CReturn({"v": "float"}):
        return 1 / (1 + i)

    assert isinstance(calc_v, Calculation)
    assert calc_v(1) == 0.5
