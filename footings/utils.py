"""utils.py"""

from attr import attrs, attrib
from attr.validators import optional, instance_of


class PriorStepError(Exception):
    """Only a name or nprior can be set, not both."""


@attrs(slots=True, frozen=True)
class PriorStep:
    """Get prior step values"""

    # pylint: disable=bare-except
    name = attrib(default=None, kw_only=True, validator=optional(instance_of(str)))
    nprior = attrib(default=None, kw_only=True, validator=optional(instance_of(int)))

    def __attrs_post_init__(self):
        pass

    def get_name(self, list_):
        """Get name from list or priors"""
        if self.nprior is not None:
            return list_[-self.nprior].name
        return self.name


GET_PRIOR_STEP = PriorStep(nprior=1)


class GetTbl:
    """Get tbl name"""


GET_TBL = GetTbl()
