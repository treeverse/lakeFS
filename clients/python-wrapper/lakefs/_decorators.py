"""
Module for renaming kwargs

Based on https://stackoverflow.com/a/74564394 but does not warn.
"""

from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar, cast

P = ParamSpec("P")
R = TypeVar("R")


def renamed_parameters(**kwarg_map) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Decorator for renaming args

    To rename old1 -> new1 and old2 -> new2, decorate the function definition:

        @renamed_parameters(old1="new1", old2="new2"})
        def func(new1, new2):
          pass
    """
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            new_kwargs = {}
            for k, v in kwargs.items():
                new_kwargs[kwarg_map.get(k, k)] = v
            return cast(Callable[..., R], func)(*args, **new_kwargs)
        return wrapped
    return decorator
