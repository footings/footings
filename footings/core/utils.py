import pandas as pd
import numpy as np
from operator import attrgetter


def _generate_message(msg, params):
    if len(params) == 1:
        m_params = params[0]
    else:
        m_params = ", ".join(str(x) for x in params)
    return msg + "[" + m_params + "]"


def is_meta_like(m):
    if type(m) == pd.DataFrame and m.shape[0] == 0:
        return True
    else:
        return False
