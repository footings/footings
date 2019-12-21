import pytest

from footings import Parameter


def test_assigned_parameter():
    s1 = Parameter(allowed=["A", "M"])
    s2 = Parameter(dtype=str, allowed=["A", "M"])
    pytest.raises(AssertionError, s1.valid, "z")
    pytest.raises(AssertionError, s2.valid, "z")
