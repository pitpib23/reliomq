"""Shared helpers for resolving deprecated names to their current replacement.

Internal module: nothing here is part of the public API. It exists so the
same "accept the old keyword, warn, and resolve to the new one" behavior is
implemented once instead of being copy-pasted across :mod:`reliomq.protocol`,
:mod:`reliomq.config`, :mod:`reliomq.sender`, and :mod:`reliomq.relay`.

Some fields have been renamed more than once across releases (e.g.
``data_topic`` (0.1) -> ``envelope_topic`` (0.2) -> ``relay_topic`` (0.3)).
:func:`resolve_renamed_argument` handles one hop; :func:`resolve_renamed_argument_chain`
handles a field with more than one deprecated predecessor name.
"""

from __future__ import annotations

import warnings
from typing import Any, TypeVar


T = TypeVar("T")


def resolve_renamed_argument(
    *,
    new_value: T | None,
    old_value: T | None,
    new_name: str,
    old_name: str,
    owner: str,
    default: T,
    error_cls: type[Exception] = ValueError,
) -> T:
    """Resolve a constructor/method argument that replaced an older name.

    Returns ``new_value`` (or ``default`` if neither was supplied). If
    ``old_value`` was supplied, a :class:`DeprecationWarning` is emitted
    pointing at ``new_name``. Supplying both with conflicting values raises
    ``error_cls``.
    """

    if old_value is not None:
        warnings.warn(
            f"{owner}({old_name}=...) is deprecated and will be removed in a "
            f"future release; use {owner}({new_name}=...) instead.",
            DeprecationWarning,
            stacklevel=3,
        )
        if new_value is not None and new_value != old_value:
            raise error_cls(
                f"cannot pass both {new_name!r} and deprecated {old_name!r} "
                "with different values"
            )
        new_value = old_value

    return default if new_value is None else new_value


def resolve_renamed_argument_chain(
    *,
    new_value: T | None,
    new_name: str,
    legacy: list[tuple[T | None, str]],
    owner: str,
    default: T,
    error_cls: type[Exception] = ValueError,
) -> T:
    """Resolve an argument with more than one deprecated predecessor name.

    ``legacy`` lists each older name as ``(value, name)``, most-recently
    deprecated first (order only affects warning order, not the result).
    Every legacy value that was actually supplied gets its own
    :class:`DeprecationWarning`. If two or more supplied values (across
    ``new_value`` and every legacy value) disagree, raises ``error_cls``.
    """

    supplied: list[tuple[T, str]] = []
    if new_value is not None:
        supplied.append((new_value, new_name))
    for value, name in legacy:
        if value is not None:
            warnings.warn(
                f"{owner}({name}=...) is deprecated and will be removed in a "
                f"future release; use {owner}({new_name}=...) instead.",
                DeprecationWarning,
                stacklevel=3,
            )
            supplied.append((value, name))

    if not supplied:
        return default

    first_value, _first_name = supplied[0]
    for value, name in supplied[1:]:
        if value != first_value:
            conflicting = ", ".join(f"{n}={v!r}" for v, n in supplied)
            raise error_cls(
                f"cannot pass conflicting values for {new_name!r}: {conflicting}"
            )
    return first_value


def warn_deprecated_attribute(*, owner: str, old_name: str, new_name: str) -> None:
    """Emit the standard warning for reading a renamed attribute/property."""

    warnings.warn(
        f"{owner}.{old_name} is deprecated and will be removed in a future "
        f"release; use {owner}.{new_name} instead.",
        DeprecationWarning,
        stacklevel=3,
    )


def deprecated_function_alias(new_func: Any, *, old_name: str, new_name: str, owner: str) -> Any:
    """Wrap ``new_func`` so calling it under its old name warns once per call."""

    def _alias(*args: Any, **kwargs: Any) -> Any:
        warnings.warn(
            f"{owner}.{old_name}() is deprecated and will be removed in a "
            f"future release; use {owner}.{new_name}() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return new_func(*args, **kwargs)

    _alias.__name__ = old_name
    _alias.__doc__ = f"Deprecated alias for :func:`{new_name}`."
    return _alias
