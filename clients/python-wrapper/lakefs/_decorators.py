"""
Module for renaming kwargs

Based on https://stackoverflow.com/a/74564394 but does not warn.
"""

from collections.abc import Callable
from functools import wraps
from typing import Any, ParamSpec, TypeVar, cast

P = ParamSpec("P")
R = TypeVar("R")


def _rename_kwargs(kwarg_map: dict[str, str], func: Callable[P, R]) -> Callable[..., R]:
    @wraps(func)
    def wrapped(*args: Any, **kwargs: Any) -> R:
        new_kwargs = {}
        for k, v in kwargs.items():
            new_k = kwarg_map.get(k, k)
            if new_k in new_kwargs:
                raise TypeError(f"{func.__qualname__}() got multiple values for argument '{new_k}'")
            new_kwargs[new_k] = v
        return cast(Callable[..., R], func)(*args, **new_kwargs)
    return wrapped


def renamed_parameters_overloaded(**kwarg_map: str) -> Callable[[Callable[P, R]], Callable[..., R]]:
    """
    Decorator for renaming args

    To rename old1 -> new1 and old2 -> new2, decorate the function definition.

    To type-check old callers, declare both signatures with @overload and
    mark the old one @deprecated:

        @overload
        def func(new1: A, new2: B): ...
        @overload
        @deprecated("old1, old2 renamed to new1, new2")
        def func(*, old1, old2): ...

        @renamed_parameters_overloaded(old1="new1", old2="new2")
        def func(new1, new2):
          pass

    Make old names keyword-only in the deprecated overload: positional calls
    already match the new signature, and mixing a positional new1 with a
    keyword old1 fails at runtime.  Use typing_extensions.deprecated, which
    supports all Python versions.

    Type checkers report calls that match the deprecated overload only when
    asked to: mypy needs --enable-error-code=deprecated, pyright needs
    reportDeprecated (enabled in strict mode).
    """
    def decorator(func: Callable[P, R]) -> Callable[..., R]:
        return _rename_kwargs(kwarg_map, func)
    return decorator
