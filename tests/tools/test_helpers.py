import pandas as pd
from pandas.testing import assert_frame_equal

from footings import create_loaded_function
from footings.tools import post_drop_columns


def test_post_drop_columns():
    def base_func():
        return pd.DataFrame({"col_1": [1, 2, 3], "col_2": [4, 5, 6], "col_3": [7, 8, 9]})

    # test against standard function
    @post_drop_columns(columns=["col_1", "col_2"])
    def create_frame():
        return base_func()

    assert_frame_equal(create_frame(), pd.DataFrame({"col_3": [7, 8, 9]}))

    # test against an already existing loaded function
    loaded_func = create_loaded_function(base_func)
    post_drop_columns(loaded_func, columns=["col_1", "col_2"])
    assert_frame_equal(loaded_func(), pd.DataFrame({"col_3": [7, 8, 9]}))
