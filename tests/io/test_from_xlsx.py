import os
from footings.io import load_footings_file, load_footings_xlsx_file
from footings.io.to_xlsx import FootingsXlsxEntry

test_dict = {"outer": {"inner": {"endpoint1": 1, "endpoint2": 2}}, "endpoint3": 3}
expected_dict = {
    "/outer/inner/endpoint1": 1,
    "/outer/inner/endpoint2": 2,
    "/endpoint3": 3,
}


def test_load_xlsx_file():
    # from footings.io.to_xlsx import FootingsXlsxWb
    # wb = FootingsXlsxWb.create()
    # wb.create_sheet("test_dict", start_row=2, start_col=2)
    # wb.write_obj("test_dict", test_dict)
    # wb.save("test-load-xlsx-file.xlsx")
    kws = {"worksheet": "test_dict", "source": None, "column_name": None, "stable": None}
    kws_str = {**kws, "dtype": "<class 'str'>"}
    kws_int = {**kws, "dtype": "<class 'int'>"}

    expected_dict = {
        FootingsXlsxEntry(
            mapping="/outer/",
            end_point="KEY",
            row_start=2,
            col_start=2,
            row_end=2,
            col_end=2,
            **kws_str,
        ): "outer",
        FootingsXlsxEntry(
            mapping="/outer/inner/",
            end_point="KEY",
            row_start=2,
            col_start=3,
            row_end=2,
            col_end=3,
            **kws_str,
        ): "inner",
        FootingsXlsxEntry(
            mapping="/outer/inner/endpoint1/",
            end_point="KEY",
            row_start=2,
            col_start=4,
            row_end=2,
            col_end=4,
            **kws_str,
        ): "endpoint1",
        FootingsXlsxEntry(
            mapping="/outer/inner/endpoint1/",
            end_point="VALUE",
            row_start=2,
            col_start=5,
            row_end=2,
            col_end=5,
            **kws_int,
        ): 1,
        FootingsXlsxEntry(
            mapping="/outer/inner/endpoint2/",
            end_point="KEY",
            row_start=3,
            col_start=4,
            row_end=3,
            col_end=4,
            **kws_str,
        ): "endpoint2",
        FootingsXlsxEntry(
            mapping="/outer/inner/endpoint2/",
            end_point="VALUE",
            row_start=3,
            col_start=5,
            row_end=3,
            col_end=5,
            **kws_int,
        ): 2,
        FootingsXlsxEntry(
            mapping="/endpoint3/",
            end_point="KEY",
            row_start=4,
            col_start=2,
            row_end=4,
            col_end=2,
            **kws_str,
        ): "endpoint3",
        FootingsXlsxEntry(
            mapping="/endpoint3/",
            end_point="VALUE",
            row_start=4,
            col_start=3,
            row_end=4,
            col_end=3,
            **kws_int,
        ): 3,
    }

    test_file = os.path.join("tests", "io", "data", "expected-load-file.xlsx")
    assert load_footings_file(test_file) == expected_dict
    assert load_footings_xlsx_file(test_file) == expected_dict
