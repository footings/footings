"""Objects to turn out a model audit to an xlsx file."""

import datetime
from collections.abc import Mapping, Iterable

from attr import attrs, attrib
import pandas as pd
import xlsxwriter

#########################################################################################
# xlsx_dispatch
#########################################################################################

_ROW_SPACING = {
    "builtins": 1,
    "series": 2,
    "dataframe": 2,
    "mapping": 2,
    "iterable": 2,
}

_COL_SPACING = {
    "builtins": 0,
    "series": 0,
    "dataframe": 0,
    "mapping": 0,
    "iterable": 0,
}


@attrs(slots=True, frozen=True, repr=False)
class XlsxRange:
    """XlsxRange"""

    row_start: int = attrib()
    col_start: int = attrib()
    row_end: int = attrib()
    col_end: int = attrib()
    row_spacing: int = attrib()
    col_spacing: int = attrib()


def _obj_to_xlsx_builtins(obj, worksheet, start_row, start_col, xlsx_format):
    worksheet.write(start_row, start_col, obj, xlsx_format)
    return XlsxRange(
        row_start=start_row,
        col_start=start_col,
        row_end=start_row,
        col_end=start_col,
        row_spacing=_ROW_SPACING["builtins"],
        col_spacing=_COL_SPACING["builtins"],
    )


def _obj_to_xlsx_series(obj, worksheet, start_row, start_col, xlsx_format):
    # write header
    worksheet.write(start_row, start_col, obj.name, xlsx_format)
    # write rows
    worksheet.write_column(start_row + 1, start_col, obj.values, xlsx_format)
    return XlsxRange(
        row_start=start_row,
        col_start=start_col,
        row_end=start_row + obj.shape[0],
        col_end=start_col,
        row_spacing=_ROW_SPACING["series"],
        col_spacing=_COL_SPACING["series"],
    )


def _obj_to_xlsx_dataframe(obj, worksheet, start_row, start_col, xlsx_format):
    for idx, column in enumerate(obj):
        _obj_to_xlsx_series(
            obj[column], worksheet, start_row, start_col + idx, xlsx_format
        )
    return XlsxRange(
        row_start=start_row,
        col_start=start_col,
        row_end=start_row + obj.shape[0],
        col_end=start_col,
        row_spacing=_ROW_SPACING["dataframe"],
        col_spacing=_COL_SPACING["dataframe"],
    )


def _obj_to_xlsx_mapping(obj, worksheet, start_row, start_col, xlsx_format):
    if len(obj) > 0:
        for idx, (k, v) in enumerate(obj.items()):
            if not isinstance(k, str):
                try:
                    k = str(k)
                except:
                    raise ValueError("Converting key of mapping to string failed.")
            if idx == 0:
                obj_to_xlsx(k, worksheet, start_row, start_col, xlsx_format)
                ret_xlsx = obj_to_xlsx(
                    v, worksheet, start_row, start_col + 1, xlsx_format
                )
            else:
                row = ret_xlsx.row_end + ret_xlsx.row_spacing
                obj_to_xlsx(k, worksheet, row, start_col, xlsx_format)
                ret_xlsx = obj_to_xlsx(v, worksheet, row, start_col + 1, xlsx_format)
        return XlsxRange(
            row_start=start_row,
            col_start=start_col,
            row_end=ret_xlsx.row_end,
            col_end=start_col,
            row_spacing=_ROW_SPACING["mapping"],
            col_spacing=_COL_SPACING["mapping"],
        )
    return XlsxRange(
        row_start=start_row,
        col_start=start_col,
        row_end=start_row,
        col_end=start_col,
        row_spacing=_ROW_SPACING["mapping"],
        col_spacing=_COL_SPACING["mapping"],
    )


def _obj_to_xlsx_iterable(obj, worksheet, start_row, start_col, xlsx_format):
    if len(obj) > 0:
        for idx, x in enumerate(obj):
            if idx == 0:
                ret_xlsx = obj_to_xlsx(x, worksheet, start_row, start_col, xlsx_format)
            else:
                ret_xlsx = obj_to_xlsx(
                    x,
                    worksheet,
                    ret_xlsx.row_end + ret_xlsx.row_spacing,
                    start_col,
                    xlsx_format,
                )
        return XlsxRange(
            row_start=start_row,
            col_start=start_col,
            row_end=ret_xlsx.row_end,
            col_end=ret_xlsx.col_end,
            row_spacing=_ROW_SPACING["iterable"],
            col_spacing=_COL_SPACING["iterable"],
        )
    return XlsxRange(
        row_start=start_row,
        col_start=start_col,
        row_end=start_row,
        col_end=start_col,
        row_spacing=_ROW_SPACING["iterable"],
        col_spacing=_COL_SPACING["iterable"],
    )


def obj_to_xlsx(
    obj, worksheet, start_row, start_col, xlsx_format
):  # pylint: disable=too-many-return-statements
    """Object to xlsx"""
    builtins = [bool, str, int, float, datetime.date, datetime.datetime]
    if any([isinstance(obj, x) for x in builtins]):
        return _obj_to_xlsx_builtins(obj, worksheet, start_row, start_col, xlsx_format)
    if isinstance(obj, pd.Series):
        return _obj_to_xlsx_series(obj, worksheet, start_row, start_col, xlsx_format)
    if isinstance(obj, pd.DataFrame):
        return _obj_to_xlsx_dataframe(obj, worksheet, start_row, start_col, xlsx_format)
    if isinstance(obj, Mapping):
        return _obj_to_xlsx_mapping(obj, worksheet, start_row, start_col, xlsx_format)
    if isinstance(obj, Iterable):
        return _obj_to_xlsx_iterable(obj, worksheet, start_row, start_col, xlsx_format)
    if hasattr(obj, "to_audit_format"):
        new_obj = obj.to_audit_format()
        return obj_to_xlsx(new_obj, worksheet, start_row, start_col, xlsx_format)
    if callable(obj):
        new_obj = "callable: " + obj.__module__ + "." + obj.__qualname__
        return obj_to_xlsx(new_obj, worksheet, start_row, start_col, xlsx_format)
    msg = ""
    raise TypeError(msg)


#########################################################################################
# to_excel
#########################################################################################


@attrs(slots=True, repr=False)
class XlsxWorksheet:
    """XlsxWorksheet"""

    obj: xlsxwriter.worksheet = attrib()
    write_row: int = attrib()
    write_col: int = attrib()


@attrs(slots=True, repr=False)
class XlsxWorkbook:
    """XlsxWorkbook"""

    workbook: xlsxwriter.Workbook = attrib()
    worksheets: dict = attrib(factory=dict)
    formats: dict = attrib(factory=dict)

    @classmethod
    def create(cls, name, **kwargs):
        """Create xlsx workbook"""
        workbook = xlsxwriter.Workbook(name, options=kwargs)
        return cls(workbook=workbook)

    def add_worksheet(self, name, start_row, start_col, **kwargs):
        """Add worksheet"""
        wrksht = XlsxWorksheet(
            obj=self.workbook.add_worksheet(name, **kwargs),
            write_row=start_row,
            write_col=start_col,
        )
        self.worksheets.update({name: wrksht})

    def add_format(self, name, **kwargs):
        """Add formats"""
        self.formats.update({name: self.workbook.add_format(kwargs)})

    def write_obj(self, worksheet, obj, add_rows=0, add_cols=0, xlsx_format=None):
        """Write object to worksheet"""
        wrksht = self.worksheets[worksheet]
        start_row = wrksht.write_row
        start_col = wrksht.write_col
        if xlsx_format is not None:
            try:
                xlsx_format = self.formats.get(xlsx_format)
            except KeyError as err:
                raise err
        ret_range = obj_to_xlsx(
            obj, wrksht.obj, start_row + add_rows, start_col + add_cols, xlsx_format
        )
        wrksht.write_row = ret_range.row_end + ret_range.row_spacing
        wrksht.write_col = start_col + ret_range.col_spacing

    def close(self):
        """Close workbook"""
        self.workbook.close()
