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


# @attrs
# class _PlanCache:
#     """Plan values to keep"""
# 
# 
# def _create_plan_cache(levels):
# 
#     names = {}
#     for step in levels:
#         if isinstance(step, TblSchema):
#             names.update({step.name: []})
#             print(names)
#         elif isinstance(step, (TblStep, TblFlight)):
#             args = {}
#             for k, v in step.args.items():
#                 if isinstance(v, _GetPrior):
#                     print("check")
#                     args.update({k: v.get_name(names)})
#             names.update({step.name: args})
#         else:
#             raise TypeError(
#                 f"Each plan step must be a {TblSchema}, {TblStep}, or {TblFlight}."
#             )
#     return names
