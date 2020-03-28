"""ffunctions.py"""

from attr import attrs, attrib
from attr.validators import optional, is_callable, instance_of


#########################################################################################
# FootingsTask
#########################################################################################


@attrs(slots=True, frozen=True, repr=False)
class FFunction:
    """FootingsTask"""

    name = attrib(validator=instance_of(str))
    test_enter = attrib(default=None, validator=optional(is_callable()))
    pre_hook = attrib(default=None, validator=optional(is_callable()))
    function = attrib(validator=is_callable())
    post_hook = attrib(default=None, validator=optional(is_callable()))
    test_exit = attrib(default=None, validator=optional(is_callable()))
