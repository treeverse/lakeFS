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
            new_kwargs[kwarg_map.get(k, k)] = v
        return cast(Callable[..., R], func)(*args, **new_kwargs)
    return wrapped


def renamed_parameters(**kwarg_map: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Decorator for renaming args

    To rename old1 -> new1 and old2 -> new2, decorate the function definition:

        @renamed_parameters(old1="new1", old2="new2")
        def func(new1, new2):
          pass

    Old names keep working at runtime, but type checkers see only the new
    signature, so they reject calls that use old names.

    If old callers must type-check, declare both signatures with @overload,
    mark the old one @deprecated, and decorate the implementation with
    renamed_parameters_overloaded instead:

        @overload
        def func(new1, new2): ...
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
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        return cast(Callable[P, R], _rename_kwargs(kwarg_map, func))
    return decorator


def renamed_parameters_overloaded(**kwarg_map: str) -> Callable[[Callable[P, R]], Callable[..., R]]:
    """
    renamed_parameters for implementations of @overload-ed functions.

    Identical at runtime.  The decorated function accepts any arguments, so that
    type checkers accept it as an implementation of the deprecated overloads that
    use old names.  Callers are still checked against the overloads.  See
    renamed_parameters for usage.
    """
    def decorator(func: Callable[P, R]) -> Callable[..., R]:
        return _rename_kwargs(kwarg_map, func)
    return decorator
