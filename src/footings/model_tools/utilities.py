import functools


# https://discuss.python.org/t/reduce-the-overhead-of-functools-lru-cache-for-functions-with-no-parameters/3956/3
def once(function):
    obj = None

    @functools.wraps(function)
    def inner():
        nonlocal obj
        if obj is None:
            obj = function()
        return obj

    return inner
