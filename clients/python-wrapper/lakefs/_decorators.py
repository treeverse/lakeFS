"""
Module for renaming kwargs

Based on https://stackoverflow.com/a/74564394 but does not warn.
"""

from functools import wraps


def re_arg(kwarg_map):
    """
    Decorator for renaming args

    To rename old1 -> new1 and old2 -> new2, decorate the function definition:

        @re_arg({"old1": "new1", "old2": "new2"})
        def func(new1, new2):
          pass
    """
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            new_kwargs = {}
            for k, v in kwargs.items():
                new_kwargs[kwarg_map.get(k, k)] = v
            return func(*args, **new_kwargs)
        return wrapped
    return decorator
