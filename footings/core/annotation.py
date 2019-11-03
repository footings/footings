class Column:
    """ 
    """

    def __init__(self, dtype):
        self.dtype = dtype


class CReturn:
    """ 
    """

    def __init__(self, col_dict: dict):
        assert len(col_dict.keys()) == 1, "CReturn cannot return multiple columns"
        self.columns = col_dict
        self.nodes = list(col_dict.keys())


class Frame:
    """ 
    """

    def __init__(self, col_dict: dict):
        self.columns = col_dict
        self.nodes = list(col_dict.keys())


class FReturn:
    """ 
    """

    def __init__(self, col_dict: dict):
        self.columns = col_dict
        self.nodes = list(col_dict.keys())


class Setting:
    """
    """

    def __init__(self, dtype=None, allowed=None, default=None):
        self.dtype = dtype
        self.allowed = allowed
        self.default = default

    def validate(self, value):
        pass


def _parse_annotation_settings(x):
    return {k: v for k, v in x.__annotations__.items() if type(v) == Setting}


def _parse_annotation_input(x):
    if type(x.__annotations__["return"]) == CReturn:
        return {k: v.dtype for k, v in x.__annotations__.items() if type(v) == Column}
    elif type(x.__annotations__["return"]) == FReturn:
        l = []
        for k, v in x.__annotations__.items():
            if type(v) == Frame:
                l.append(v.columns)
        assert len(l) == 1, "check frame"
        return l[0]


def _parse_annotation_output(x):
    return x.__annotations__["return"].columns
