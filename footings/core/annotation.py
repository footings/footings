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
