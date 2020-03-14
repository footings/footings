"""errors.py"""


class FootingsNestedTaskError(Exception):
    """Error creating NestedTask."""


class FootingsTaskCreationError(Exception):
    """Error raised creating FootingsTask."""


class FootingsTaskEnterError(Exception):
    """Error raised running FootingsTask on enter."""


class FootingsTaskExitError(Exception):
    """Error raised running FootingsTask on exit."""


class FootingsTaskCallError(Exception):
    """Error raised trying to call FootingsTask."""


class FootingsDispatcherKeyError(Exception):
    """Key does not exist within dispatch function registry."""


class FootingsDispatcherRegisterError(Exception):
    """Error occuring registering a function to the Dispatcher."""
