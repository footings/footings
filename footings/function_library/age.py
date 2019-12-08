import numpy as np
from operator import attrgetter


def calc_age(birth_dt, as_of_dt, method):
    """

    """
    assert method in ["ANB", "ALB", "ACB"], "method must be one of 'ANB', 'ALB', 'ACB'"
    assert all(as_of_dt.dt > birth_dt.dt), "as_of_dt must be greater than birth_dt"

    diff = (as_of_dt.dt.to_period("M") - birth_dt.dt.to_period("M")).apply(
        attrgetter("n")
    ) / 12

    if method == "ANB":
        return np.ceil(diff).astype(int)
    if method == "ALB":
        return np.floor(diff).astype(int)
    if method == "ACB":
        return np.round(diff, 0).astype(int)
