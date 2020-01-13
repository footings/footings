from typing import List, Dict, Tuple, Union, Optional, Any, Callable
from dataclasses import dataclass, field


@dataclass
class ColumnSchema:
    name: str
    description: Optional[str] = field(default=None, repr=False)
    nullable: Optional[bool] = field(default=None)
    dtype: Optional[type] = field(default=Any)
    allowed: Optional[List[Any]] = field(default=None)
    min_val: Optional[Union[int, float]] = field(default=None)
    max_val: Optional[Union[int, float]] = field(default=None)
    min_len: Optional[int] = field(default=None)
    max_len: Optional[int] = field(default=None)
    custom: Optional[Callable] = field(default=None)

    def valid(self, value):
        return True

    def to_pandas_series(self):
        pass


@dataclass
class TableSchema:
    name: str
    description: str
    columns: List[ColumnSchema]
    custom: Optional[Callable]

    def valid(self, value):
        return True

    def to_pandas_dataframe(self):
        pass


def table_schema_from_yaml(file):
    pass


def table_schema_from_json(file):
    pass
